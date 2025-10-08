package hls

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/mp4"
)

type Session struct {
	cons           core.Consumer
	id             string
	template       string
	init           []byte
	buffer         []byte
	seq            int
	alive          *time.Timer
	mu             sync.Mutex
	segmentManager *SegmentManager
	isPreloaded    bool
}

func NewSession(cons core.Consumer) *Session {
	s := &Session{
		id:   core.RandString(8, 62),
		cons: cons,
	}

	// two segments important for Chromecast
	if _, ok := cons.(*mp4.Consumer); ok {
		s.template = `#EXTM3U
#EXT-X-VERSION:6
#EXT-X-TARGETDURATION:1
#EXT-X-MEDIA-SEQUENCE:%d
#EXT-X-MAP:URI="init.mp4?id=` + s.id + `"
#EXTINF:0.500,
segment.m4s?id=` + s.id + `&n=%d
#EXTINF:0.500,
segment.m4s?id=` + s.id + `&n=%d`
	} else {
		s.template = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:1
#EXT-X-MEDIA-SEQUENCE:%d
#EXTINF:0.500,
segment.ts?id=` + s.id + `&n=%d
#EXTINF:0.500,
segment.ts?id=` + s.id + `&n=%d`
	}

	return s
}

func (s *Session) EnablePreload() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.isPreloaded {
		s.isPreloaded = true
		s.segmentManager = NewSegmentManager(s.id, s.cons)
		s.segmentManager.StartRecording()
	}
}

func (s *Session) IsPreloaded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isPreloaded
}

func (s *Session) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.segmentManager != nil {
		s.segmentManager.Cleanup()
	}
}

func (s *Session) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	if s.init == nil {
		s.init = p
	} else {
		s.buffer = append(s.buffer, p...)
	}
	s.mu.Unlock()
	return len(p), nil
}

func (s *Session) Run() {
	_, _ = s.cons.(io.WriterTo).WriteTo(s)
}

func (s *Session) Main() []byte {
	type withCodecs interface {
		Codecs() []*core.Codec
	}

	codecs := mp4.MimeCodecs(s.cons.(withCodecs).Codecs())
	codecs = strings.Replace(codecs, mp4.MimeFlac, "fLaC", 1)

	// bandwidth important for Safari, codecs useful for smooth playback
	return []byte(`#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=192000,CODECS="` + codecs + `"
hls/playlist.m3u8?id=` + s.id)
}

func (s *Session) Playlist() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// If preloaded, use segment manager's playlist
	if s.isPreloaded && s.segmentManager != nil {
		if data, err := s.segmentManager.GetPlaylist(); err == nil {
			return data
		}
	}
	
	// Fallback to original behavior
	return []byte(fmt.Sprintf(s.template, s.seq, s.seq, s.seq+1))
}

func (s *Session) Init() (init []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// If preloaded, use segment manager's init
	if s.isPreloaded && s.segmentManager != nil {
		if data, err := s.segmentManager.GetInit(); err == nil {
			return data
		}
	}
	
	// Fallback to original behavior
	for i := 0; i < 60 && init == nil; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond)
		}

		// return init only when have some buffer
		if len(s.buffer) > 0 {
			init = s.init
		}
	}

	return
}

func (s *Session) Segment() (segment []byte) {
	for i := 0; i < 60 && segment == nil; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond)
		}

		s.mu.Lock()
		if len(s.buffer) > 0 {
			segment = s.buffer
			if _, ok := s.cons.(*mp4.Consumer); ok {
				s.buffer = nil
			} else {
				// for TS important to start new segment with init
				s.buffer = s.init
			}
			s.seq++
		}
		s.mu.Unlock()
	}

	return
}

func (s *Session) GetSegmentByFilename(filename string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.isPreloaded && s.segmentManager != nil {
		return s.segmentManager.GetSegment(filename)
	}
	
	return nil, fmt.Errorf("segment not available")
}
