package hls

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/api/ws"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/mp4"
	"github.com/AlexxIT/go2rtc/pkg/mpegts"
	"github.com/AlexxIT/go2rtc/pkg/probe"
	"github.com/AlexxIT/go2rtc/pkg/shell"
	"github.com/rs/zerolog"
)

func Init() {
	log = app.GetLogger("hls")

	// Clean up any leftover preload directories from previous runs
	CleanupAllPreloadDirectories()

	// Register callback for preload recording
	streams.SetHLSPreloadCallback(startPreloadRecording)

	api.HandleFunc("api/stream.m3u8", handlerStream)
	api.HandleFunc("api/hls/playlist.m3u8", handlerPlaylist)
	api.HandleFunc("api/hls/preload.m3u8", handlerPreloadPlaylist)

	// HLS (TS)
	api.HandleFunc("api/hls/segment.ts", handlerSegmentTS)

	// HLS (fMP4)
	api.HandleFunc("api/hls/init.mp4", handlerInit)
	api.HandleFunc("api/hls/segment.m4s", handlerSegmentMP4)
	
	// Preload handlers
	api.HandleFunc("api/hls/preload_segment", handlerPreloadSegment)
	api.HandleFunc("api/hls/preload_init.mp4", handlerPreloadInit)

	ws.HandleFunc("hls", handlerWSHLS)
}

var log zerolog.Logger

const keepalive = 5 * time.Second

// once I saw 404 on MP4 segment, so better to use mutex
var sessions = map[string]*Session{}
var sessionsMu sync.RWMutex

func handlerStream(w http.ResponseWriter, r *http.Request) {
	// CORS important for Chromecast
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	src := r.URL.Query().Get("src")
	stream := streams.Get(src)
	if stream == nil {
		http.Error(w, api.StreamNotFound, http.StatusNotFound)
		return
	}

	// Check if this stream has preload enabled
	if streams.HasPreload(stream) {
		// For preloaded streams, return a playlist that points to the shared preload playlist
		// The recording should already be running since preload was configured
		
		preloadedMu.RLock()
		_, exists := preloadedStreams[src]
		preloadedMu.RUnlock()
		
		if !exists {
			// This shouldn't happen if preload is properly configured, but handle it gracefully
			log.Warn().Str("src", src).Msg("[hls] preloaded stream not found, but preload is configured")
			http.Error(w, "preload not ready", http.StatusServiceUnavailable)
			return
		}
		
		// Return master playlist that points to the preload playlist
		response := fmt.Sprintf(`#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=192000
hls/preload.m3u8?src=%s`, src)
		
		if _, err := w.Write([]byte(response)); err != nil {
			log.Error().Err(err).Caller().Send()
		}
		return
	}

	// Original logic for non-preloaded streams
	var cons core.Consumer

	// use fMP4 with codecs filter and TS without
	medias := mp4.ParseQuery(r.URL.Query())
	if medias != nil {
		c := mp4.NewConsumer(medias)
		c.FormatName = "hls/fmp4"
		c.WithRequest(r)
		cons = c
	} else {
		c := mpegts.NewConsumer()
		c.FormatName = "hls/mpegts"
		c.WithRequest(r)
		cons = c
	}

	if err := stream.AddConsumer(cons); err != nil {
		log.Error().Err(err).Caller().Send()
		return
	}

	session := NewSession(cons)
	session.alive = time.AfterFunc(keepalive, func() {
		sessionsMu.Lock()
		if s, exists := sessions[session.id]; exists {
			s.Cleanup()
			delete(sessions, session.id)
		}
		sessionsMu.Unlock()

		stream.RemoveConsumer(cons)
	})

	sessionsMu.Lock()
	sessions[session.id] = session
	sessionsMu.Unlock()

	go session.Run()

	if _, err := w.Write(session.Main()); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

func checkStreamPreload(src string) bool {
	// Check if the stream is configured with preload in the streams package
	// This will be integrated with the streams package preload functionality
	stream := streams.Get(src)
	if stream == nil {
		return false
	}
	return streams.HasPreload(stream)
}

func handlerPreloadPlaylist(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	src := r.URL.Query().Get("src")
	if src == "" {
		http.Error(w, "src parameter required", http.StatusBadRequest)
		return
	}

	preloadedMu.RLock()
	ps, exists := preloadedStreams[src]
	preloadedMu.RUnlock()
	
	if !exists {
		http.Error(w, "preloaded stream not found", http.StatusNotFound)
		return
	}

	data, err := ps.GetPlaylist()
	if err != nil {
		log.Error().Err(err).Str("src", src).Msg("[hls] failed to get preload playlist")
		http.Error(w, "failed to get playlist", http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

func handlerPreloadSegment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "video/mp4")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	src := r.URL.Query().Get("src")
	if src == "" {
		http.Error(w, "src parameter required", http.StatusBadRequest)
		return
	}

	filename := r.URL.Query().Get("filename")
	if filename == "" {
		http.Error(w, "filename parameter required", http.StatusBadRequest)
		return
	}
	
	preloadedMu.RLock()
	ps, exists := preloadedStreams[src]
	preloadedMu.RUnlock()
	
	if !exists {
		http.Error(w, "preloaded stream not found", http.StatusNotFound)
		return
	}

	data, err := ps.GetSegment(filename)
	if err != nil {
		log.Warn().Err(err).Str("src", src).Str("segment", filename).Msg("[hls] can't get preload segment")
		http.NotFound(w, r)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}


func handlerPreloadInit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "video/mp4")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	src := r.URL.Query().Get("src")
	if src == "" {
		http.Error(w, "src parameter required", http.StatusBadRequest)
		return
	}

	preloadedMu.RLock()
	ps, exists := preloadedStreams[src]
	preloadedMu.RUnlock()
	
	if !exists {
		http.Error(w, "preloaded stream not found", http.StatusNotFound)
		return
	}

	data, err := ps.GetInit()
	if err != nil {
		log.Warn().Err(err).Str("src", src).Msg("[hls] can't get preload init")
		http.NotFound(w, r)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

// startPreloadRecording is called by the streams package when a preload is configured
func startPreloadRecording(src string, cons *probe.Probe, query url.Values) {
	log.Debug().Str("src", src).Msg("[hls] starting preload recording")
	
	// For preload, default to MP4 format since it's more modern and efficient
	// This matches the expectation when clients request with mp4=flac parameter
	isMP4 := true
	
	// Only use TS if explicitly configured in preload query
	if query.Has("format") && query.Get("format") == "ts" {
		isMP4 = false
	}
	
	// Get or create the preloaded stream
	ps := GetOrCreatePreloadedStream(src, isMP4)
	
	// Get the actual stream
	stream := streams.Get(src)
	if stream == nil {
		log.Error().Str("src", src).Msg("[hls] stream not found for preload recording")
		return
	}
	
	// Create a consumer for recording
	var hlsConsumer core.Consumer
	if isMP4 {
		// Use default medias for MP4 consumer (will handle various codecs)
		medias := []*core.Media{
			{
				Kind:      core.KindVideo,
				Direction: core.DirectionSendonly,
				Codecs:    []*core.Codec{{Name: core.CodecAny}},
			},
			{
				Kind:      core.KindAudio,
				Direction: core.DirectionSendonly,
				Codecs:    []*core.Codec{{Name: core.CodecAny}},
			},
		}
		c := mp4.NewConsumer(medias)
		c.FormatName = "hls/fmp4/preload"
		hlsConsumer = c
	} else {
		c := mpegts.NewConsumer()
		c.FormatName = "hls/mpegts/preload"
		hlsConsumer = c
	}
	
	// Add the consumer to the stream
	if err := stream.AddConsumer(hlsConsumer); err != nil {
		log.Error().Err(err).Str("src", src).Msg("[hls] failed to add preload consumer")
		return
	}
	
	// Start recording
	ps.StartRecording(hlsConsumer)
	
	log.Info().Str("src", src).Bool("isMP4", isMP4).Msg("[hls] preload recording started")
}

func handlerPlaylist(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	sid := r.URL.Query().Get("id")
	sessionsMu.RLock()
	session := sessions[sid]
	sessionsMu.RUnlock()
	if session == nil {
		http.NotFound(w, r)
		return
	}

	if _, err := w.Write(session.Playlist()); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

func handlerSegmentTS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "video/mp2t")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	query := r.URL.Query()
	sid := query.Get("id")
	filename := query.Get("filename")
	
	sessionsMu.RLock()
	session := sessions[sid]
	sessionsMu.RUnlock()
	if session == nil {
		http.NotFound(w, r)
		return
	}

	session.alive.Reset(keepalive)

	var data []byte
	
	// If filename is provided and session is preloaded, serve from file
	if filename != "" && session.IsPreloaded() {
		if segmentData, err := session.GetSegmentByFilename(filename); err == nil {
			data = segmentData
		}
	}
	
	// Fallback to original behavior
	if data == nil {
		data = session.Segment()
	}
	
	if data == nil {
		log.Warn().Msgf("[hls] can't get segment %s", shell.Redact(r.URL.RawQuery))
		http.NotFound(w, r)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

func handlerInit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "video/mp4")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	sid := r.URL.Query().Get("id")
	sessionsMu.RLock()
	session := sessions[sid]
	sessionsMu.RUnlock()
	if session == nil {
		http.NotFound(w, r)
		return
	}

	data := session.Init()
	if data == nil {
		log.Warn().Msgf("[hls] can't get init %s", shell.Redact(r.URL.RawQuery))
		http.NotFound(w, r)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}

func handlerSegmentMP4(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "video/iso.segment")

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		return
	}

	query := r.URL.Query()
	sid := query.Get("id")
	filename := query.Get("filename")

	sessionsMu.RLock()
	session := sessions[sid]
	sessionsMu.RUnlock()
	if session == nil {
		http.NotFound(w, r)
		return
	}

	session.alive.Reset(keepalive)

	var data []byte
	
	// If filename is provided and session is preloaded, serve from file
	if filename != "" && session.IsPreloaded() {
		if segmentData, err := session.GetSegmentByFilename(filename); err == nil {
			data = segmentData
		}
	}
	
	// Fallback to original behavior
	if data == nil {
		data = session.Segment()
	}

	if data == nil {
		log.Warn().Msgf("[hls] can't get segment %s", shell.Redact(r.URL.RawQuery))
		http.NotFound(w, r)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Caller().Send()
	}
}
