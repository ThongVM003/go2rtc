package hls

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

// Global storage for preloaded stream segment managers
var (
	preloadedStreams = map[string]*PreloadedStream{}
	preloadedMu      sync.RWMutex
)

// CleanupAllPreloadDirectories removes all preload directories from previous runs
func CleanupAllPreloadDirectories() {
	preloadBaseDir := filepath.Join(os.TempDir(), "go2rtc_hls_preload")
	if _, err := os.Stat(preloadBaseDir); err == nil {
		log.Info().Str("dir", preloadBaseDir).Msg("[hls] cleaning up all preload directories from previous runs")
		if err := os.RemoveAll(preloadBaseDir); err != nil {
			log.Warn().Err(err).Str("dir", preloadBaseDir).Msg("[hls] failed to clean up preload directories")
		}
	}
}

type PreloadedStream struct {
	src          string
	baseDir      string
	tempDir      string
	playlistPath string
	segments     []string
	sequence     int     // Used for HLS playlist generation (#EXT-X-MEDIA-SEQUENCE), not for filename uniqueness
	isMP4        bool
	initData     []byte
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	consumer     core.Consumer
	recording    bool
}

func GetOrCreatePreloadedStream(src string, isMP4 bool) *PreloadedStream {
	preloadedMu.Lock()
	defer preloadedMu.Unlock()
	
	if ps, exists := preloadedStreams[src]; exists {
		return ps
	}
	
	// Create new preloaded stream
	baseDir := filepath.Join(os.TempDir(), "go2rtc_hls_preload", src)
	
	// Clean up any existing files from previous runs
	if _, err := os.Stat(baseDir); err == nil {
		log.Info().Str("src", src).Str("dir", baseDir).Msg("[hls] cleaning up existing preload directory for stream")
		if err := os.RemoveAll(baseDir); err != nil {
			log.Warn().Err(err).Str("src", src).Str("dir", baseDir).Msg("[hls] failed to clean up existing preload directory")
		}
	}
	
	// Create directory structure
	os.MkdirAll(baseDir, 0755)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	ps := &PreloadedStream{
		src:          src,
		baseDir:      baseDir,
		tempDir:      baseDir,
		playlistPath: filepath.Join(baseDir, "playlist.m3u8"),
		segments:     make([]string, 0, maxSegments),
		sequence:     0,
		isMP4:        isMP4,
		ctx:          ctx,
		cancel:       cancel,
	}
	
	preloadedStreams[src] = ps
	return ps
}

func (ps *PreloadedStream) StartRecording(consumer core.Consumer) {
	ps.mu.Lock()
	if ps.recording {
		ps.mu.Unlock()
		return
	}
	ps.recording = true
	ps.consumer = consumer
	ps.mu.Unlock()

	// Create initial empty playlist
	ps.updatePlaylist()
	
	go ps.recordSegments()
}

func (ps *PreloadedStream) StopRecording() {
	ps.mu.Lock()
	ps.recording = false
	ps.mu.Unlock()
	
	if ps.cancel != nil {
		ps.cancel()
	}
}

func (ps *PreloadedStream) recordSegments() {
	// Create a segment writer that will handle timed rotation
	segmentWriter := &PreloadSegmentWriter{
		preloadedStream: ps,
		maxDuration:     segmentDuration,
	}
	
	// Start the consumer to write data to our segment writer
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Str("src", ps.src).Msg("[hls] preload segment recording panic")
			}
			segmentWriter.Close()
		}()
		
		// Start the consumer - it will call Write() on our segmentWriter
		if writeTo, ok := ps.consumer.(io.WriterTo); ok {
			_, err := writeTo.WriteTo(segmentWriter)
			if err != nil {
				log.Error().Err(err).Str("src", ps.src).Msg("[hls] error writing to preload segment writer")
			}
		} else {
			log.Error().Str("src", ps.src).Msg("[hls] consumer does not implement WriterTo interface")
		}
	}()

	// Monitor recording state
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ps.ctx.Done():
			return
		case <-ticker.C:
			ps.mu.Lock()
			if !ps.recording {
				ps.mu.Unlock()
				return
			}
			ps.mu.Unlock()
		}
	}
}


func (ps *PreloadedStream) saveSegment(data []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	var ext string
	if ps.isMP4 {
		ext = ".m4s"
	} else {
		ext = ".ts"
	}
	
	// Use timestamp-based naming to ensure unique filenames
	// Only using timestamp to avoid sequence overflow since we only keep maxSegments (5) files
	timestamp := time.Now().Unix()
	filename := fmt.Sprintf("%s_%d%s", segmentPrefix, timestamp, ext)
	segmentPath := filepath.Join(ps.baseDir, filename)
	
	log.Debug().Str("src", ps.src).Str("path", segmentPath).Int("size", len(data)).Int64("timestamp", timestamp).Msg("[hls] writing segment file")
	
	// Write segment file
	if err := os.WriteFile(segmentPath, data, 0644); err != nil {
		log.Error().Err(err).Str("src", ps.src).Str("path", segmentPath).Msg("[hls] failed to write segment file")
		return err
	}
	
	// Update segments list
	ps.segments = append(ps.segments, filename)
	ps.sequence++
	
	log.Debug().Str("src", ps.src).Str("filename", filename).Int("sequence", ps.sequence).Msg("[hls] segment saved successfully")
	
	// Keep only last maxSegments
	if len(ps.segments) > maxSegments {
		// Remove old segment file
		oldFilename := ps.segments[0]
		oldSegmentPath := filepath.Join(ps.baseDir, oldFilename)
		if err := os.Remove(oldSegmentPath); err != nil {
			log.Warn().Err(err).Str("src", ps.src).Str("path", oldSegmentPath).Msg("[hls] failed to remove old segment file")
		} else {
			log.Debug().Str("src", ps.src).Str("filename", oldFilename).Msg("[hls] removed old segment file")
		}
		
		// Remove from list
		ps.segments = ps.segments[1:]
	}
	
	// Update playlist
	return ps.updatePlaylist()
}

func (ps *PreloadedStream) updatePlaylist() error {
	var playlist strings.Builder
	
	if ps.isMP4 {
		playlist.WriteString("#EXTM3U\n")
		playlist.WriteString("#EXT-X-VERSION:6\n")
		playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(segmentDuration.Seconds())))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", ps.sequence-len(ps.segments)))
		playlist.WriteString("#EXT-X-MAP:URI=\"preload_init.mp4?src=" + ps.src + "\"\n")
		
		for _, segment := range ps.segments {
			playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segmentDuration.Seconds()))
			playlist.WriteString(fmt.Sprintf("preload_segment?src=%s&filename=%s\n", ps.src, segment))
		}
	} else {
		playlist.WriteString("#EXTM3U\n")
		playlist.WriteString("#EXT-X-VERSION:3\n")
		playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(segmentDuration.Seconds())))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", ps.sequence-len(ps.segments)))
		
		for _, segment := range ps.segments {
			playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segmentDuration.Seconds()))
			playlist.WriteString(fmt.Sprintf("preload_segment?src=%s&filename=%s\n", ps.src, segment))
		}
	}
	
	playlistContent := playlist.String()
	log.Debug().Str("src", ps.src).Str("path", ps.playlistPath).Int("segments", len(ps.segments)).Msg("[hls] updating playlist file")
	
	err := os.WriteFile(ps.playlistPath, []byte(playlistContent), 0644)
	if err != nil {
		log.Error().Err(err).Str("src", ps.src).Str("path", ps.playlistPath).Msg("[hls] failed to write playlist file")
	}
	return err
}

func (ps *PreloadedStream) GetPlaylist() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	return os.ReadFile(ps.playlistPath)
}

func (ps *PreloadedStream) GetSegment(filename string) ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	// Look in temp directory
	filepath := path.Join(ps.tempDir, filename)
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("segment not found: %s", filename)
	}

	return data, nil
}


func (ps *PreloadedStream) GetPlaylistForStreamAI() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	// Generate playlist with relative URLs for /stream/ai/ endpoint
	var playlist strings.Builder
	
	if ps.isMP4 {
		playlist.WriteString("#EXTM3U\n")
		playlist.WriteString("#EXT-X-VERSION:6\n")
		playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(segmentDuration.Seconds())))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", ps.sequence-len(ps.segments)))
		playlist.WriteString("#EXT-X-MAP:URI=\"init.mp4\"\n")
		
		for _, segment := range ps.segments {
			playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segmentDuration.Seconds()))
			playlist.WriteString(fmt.Sprintf("%s\n", segment))
		}
	} else {
		playlist.WriteString("#EXTM3U\n")
		playlist.WriteString("#EXT-X-VERSION:3\n")
		playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(segmentDuration.Seconds())))
		playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", ps.sequence-len(ps.segments)))
		
		for _, segment := range ps.segments {
			playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segmentDuration.Seconds()))
			playlist.WriteString(fmt.Sprintf("%s\n", segment))
		}
	}
	
	return []byte(playlist.String()), nil
}

func (ps *PreloadedStream) GetInit() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.initData == nil {
		return nil, fmt.Errorf("init segment not available")
	}

	return ps.initData, nil
}

func (ps *PreloadedStream) Cleanup() {
	ps.StopRecording()
	
	// Clean up files
	if ps.baseDir != "" {
		os.RemoveAll(ps.baseDir)
	}
	
	// Remove from global map
	preloadedMu.Lock()
	delete(preloadedStreams, ps.src)
	preloadedMu.Unlock()
}

// PreloadSegmentWriter handles writing data to segment files for preloaded streams
type PreloadSegmentWriter struct {
	preloadedStream *PreloadedStream
	currentData     []byte
	startTime       time.Time
	maxDuration     time.Duration
	mu              sync.Mutex
}

func (psw *PreloadSegmentWriter) Write(p []byte) (n int, err error) {
	psw.mu.Lock()
	defer psw.mu.Unlock()
	
	log.Debug().Str("src", psw.preloadedStream.src).Int("bytes", len(p)).Msg("[hls] preload segment writer received data")
	
	// For MP4 streams, the first packet is the init segment
	if psw.preloadedStream.isMP4 && psw.preloadedStream.initData == nil {
		psw.preloadedStream.mu.Lock()
		if psw.preloadedStream.initData == nil {
			psw.preloadedStream.initData = make([]byte, len(p))
			copy(psw.preloadedStream.initData, p)
			log.Debug().Str("src", psw.preloadedStream.src).Int("size", len(p)).Msg("[hls] captured MP4 init segment")
		}
		psw.preloadedStream.mu.Unlock()
		return len(p), nil
	}
	
	// Initialize first segment if needed
	if psw.startTime.IsZero() {
		psw.startNewSegment()
		log.Debug().Str("src", psw.preloadedStream.src).Msg("[hls] started first segment")
	}
	
	// Append data to current segment
	psw.currentData = append(psw.currentData, p...)
	
	// Check if we should rotate the segment based on time
	if psw.shouldRotateSegment() {
		log.Debug().Str("src", psw.preloadedStream.src).Msg("[hls] rotating segment")
		if err := psw.rotateSegment(); err != nil {
			return len(p), err
		}
	}
	
	return len(p), nil
}

func (psw *PreloadSegmentWriter) startNewSegment() {
	psw.startTime = time.Now()
	psw.currentData = nil
}

func (psw *PreloadSegmentWriter) shouldRotateSegment() bool {
	// Rotate based on time duration
	return time.Since(psw.startTime) >= psw.maxDuration
}

func (psw *PreloadSegmentWriter) rotateSegment() error {
	if len(psw.currentData) > 0 {
		log.Debug().Str("src", psw.preloadedStream.src).Int("size", len(psw.currentData)).Msg("[hls] saving segment")
		if err := psw.preloadedStream.saveSegment(psw.currentData); err != nil {
			return err
		}
	}
	
	psw.startNewSegment()
	return nil
}

func (psw *PreloadSegmentWriter) Close() {
	psw.mu.Lock()
	defer psw.mu.Unlock()
	
	// Save any remaining data as the final segment
	if len(psw.currentData) > 0 {
		psw.rotateSegment()
	}
}
