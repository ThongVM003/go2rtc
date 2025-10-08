package hls

import (
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/internal/streams"
)

func injectPreloadToSession(session *Session, src string, preloadCons core.Consumer) {
	log.Info().Str("src", src).Msg("[hls] preload stream detected - enabling continuous recording")
	
	// Enable preload mode for this session
	session.EnablePreload()
	
	go func() {
		// Give the consumer a moment to start
		time.Sleep(100 * time.Millisecond)
		
		log.Debug().Msg("[hls] preload mode enabled with continuous segment recording")
	}()
}

// Modified to trigger preload when HLS session is created for a preloaded stream
func EnablePreloadForStream(src string, session *Session) {
	stream := streams.Get(src)
	if stream != nil && streams.HasPreload(stream) {
		injectPreloadToSession(session, src, session.cons)
	}
}
