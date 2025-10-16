package hls

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/mp4"
)

const (
	segmentDuration = 4 * time.Second
	maxSegments     = 10
	segmentPrefix   = "segment"
)

type SegmentManager struct {
	id            string
	baseDir       string
	playlistPath  string
	segments      []string
	sequence      int
	isMP4         bool
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	consumer      core.Consumer
	recording     bool
}

func NewSegmentManager(sessionID string, consumer core.Consumer) *SegmentManager {
	baseDir := filepath.Join(os.TempDir(), "go2rtc_hls", sessionID)
	os.MkdirAll(baseDir, 0755)

	ctx, cancel := context.WithCancel(context.Background())
	_, isMP4 := consumer.(*mp4.Consumer)

	sm := &SegmentManager{
		id:           sessionID,
		baseDir:      baseDir,
		playlistPath: filepath.Join(baseDir, "playlist.m3u8"),
		segments:     make([]string, 0, maxSegments),
		sequence:     0,
		isMP4:        isMP4,
		ctx:          ctx,
		cancel:       cancel,
		consumer:     consumer,
	}

	return sm
}

func (sm *SegmentManager) StartRecording() {
	sm.mu.Lock()
	if sm.recording {
		sm.mu.Unlock()
		return
	}
	sm.recording = true
	sm.mu.Unlock()

	go sm.recordSegments()
}

func (sm *SegmentManager) StopRecording() {
	sm.mu.Lock()
	sm.recording = false
	sm.mu.Unlock()
	
	if sm.cancel != nil {
		sm.cancel()
	}
}

func (sm *SegmentManager) recordSegments() {
	ticker := time.NewTicker(segmentDuration)
	defer ticker.Stop()

	// Create a writer that will collect data for the current segment
	segmentWriter := &SegmentWriter{
		segmentManager: sm,
		maxDuration:    segmentDuration,
	}
	
	// Start the consumer to write data to our segment writer
	go func() {
		if writeTo, ok := sm.consumer.(interface{ WriteTo(w interface{}) (int64, error) }); ok {
			writeTo.WriteTo(segmentWriter)
		}
	}()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.Lock()
			if !sm.recording {
				sm.mu.Unlock()
				return
			}
			
			// Force segment rotation
			segmentWriter.RotateSegment()
			sm.mu.Unlock()
		}
	}
}

func (sm *SegmentManager) saveSegment(data []byte) error {
	var ext string
	if sm.isMP4 {
		ext = ".m4s"
	} else {
		ext = ".ts"
	}
	
	filename := fmt.Sprintf("%s_%d%s", segmentPrefix, sm.sequence, ext)
	segmentPath := filepath.Join(sm.baseDir, filename)
	
	// Write segment file
	if err := os.WriteFile(segmentPath, data, 0644); err != nil {
		return err
	}
	
	// Update segments list
	sm.segments = append(sm.segments, filename)
	sm.sequence++
	
	// Keep only last maxSegments
	if len(sm.segments) > maxSegments {
		// Remove old segment file
		oldSegmentPath := filepath.Join(sm.baseDir, sm.segments[0])
		os.Remove(oldSegmentPath)
		
		// Remove from list
		sm.segments = sm.segments[1:]
	}
	
	// Update playlist
	return sm.updatePlaylist()
}

func (sm *SegmentManager) updatePlaylist() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	var playlist strings.Builder
	
	if sm.isMP4 {
		playlist.WriteString("#EXTM3U\n")
		playlist.WriteString("#EXT-X-VERSION:6\n")
		playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(segmentDuration.Seconds())))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", sm.sequence-len(sm.segments)))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"init.mp4?id=%s\"\n", sm.id))
		
		for _, segment := range sm.segments {
			playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segmentDuration.Seconds()))
			// Use the standard HLS API endpoint but pass filename as parameter
			playlist.WriteString(fmt.Sprintf("segment.m4s?id=%s&filename=%s\n", sm.id, segment))
		}
	} else {
		playlist.WriteString("#EXTM3U\n")
		playlist.WriteString("#EXT-X-VERSION:3\n")
		playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(segmentDuration.Seconds())))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", sm.sequence-len(sm.segments)))
		
		for _, segment := range sm.segments {
			playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segmentDuration.Seconds()))
			// Use the standard HLS API endpoint but pass filename as parameter
			playlist.WriteString(fmt.Sprintf("segment.ts?id=%s&filename=%s\n", sm.id, segment))
		}
	}
	
	return os.WriteFile(sm.playlistPath, []byte(playlist.String()), 0644)
}

func (sm *SegmentManager) GetPlaylist() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	return os.ReadFile(sm.playlistPath)
}

func (sm *SegmentManager) GetSegment(filename string) ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	// Validate that the segment exists in our list
	found := false
	for _, seg := range sm.segments {
		if seg == filename {
			found = true
			break
		}
	}
	
	if !found {
		return nil, fmt.Errorf("segment not found")
	}
	
	segmentPath := filepath.Join(sm.baseDir, filename)
	return os.ReadFile(segmentPath)
}

func (sm *SegmentManager) GetInit() ([]byte, error) {
	// For MP4, we need to generate init segment
	if sm.isMP4 {
		// Wait for consumer to have init data
		for i := 0; i < 60; i++ {
			if session, ok := sessions[sm.id]; ok {
				if init := session.Init(); init != nil {
					return init, nil
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil, fmt.Errorf("init segment not available")
	}
	return nil, fmt.Errorf("init segment not supported for TS")
}

func (sm *SegmentManager) Cleanup() {
	sm.StopRecording()
	
	// Clean up files
	if sm.baseDir != "" {
		os.RemoveAll(sm.baseDir)
	}
}

// SegmentWriter handles writing data to segment files
type SegmentWriter struct {
	segmentManager *SegmentManager
	currentFile    *os.File
	currentData    []byte
	startTime      time.Time
	maxDuration    time.Duration
	mu             sync.Mutex
}

func (sw *SegmentWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	
	// Initialize first segment if needed
	if sw.currentFile == nil {
		if err := sw.startNewSegment(); err != nil {
			return 0, err
		}
	}
	
	// Append data to current segment
	sw.currentData = append(sw.currentData, p...)
	
	// Check if we should rotate the segment based on size or time
	if sw.shouldRotateSegment() {
		if err := sw.rotateSegment(); err != nil {
			return len(p), err
		}
	}
	
	return len(p), nil
}

func (sw *SegmentWriter) startNewSegment() error {
	sw.startTime = time.Now()
	sw.currentData = nil
	return nil
}

func (sw *SegmentWriter) shouldRotateSegment() bool {
	// Rotate based on time duration
	return time.Since(sw.startTime) >= sw.maxDuration
}

func (sw *SegmentWriter) rotateSegment() error {
	if len(sw.currentData) > 0 {
		if err := sw.segmentManager.saveSegment(sw.currentData); err != nil {
			return err
		}
	}
	
	return sw.startNewSegment()
}

func (sw *SegmentWriter) RotateSegment() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.rotateSegment()
}

func (sw *SegmentWriter) Close() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	
	// Save any remaining data as the final segment
	if len(sw.currentData) > 0 {
		sw.rotateSegment()
	}
}
