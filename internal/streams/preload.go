package streams

import (
	"errors"
	"net/url"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/probe"
)

var preloads = map[*Stream]*probe.Probe{}
var preloadsMu sync.Mutex

func Preload(stream *Stream, rawQuery string) {
	if err := AddPreload(stream, rawQuery); err != nil {
		log.Error().Err(err).Caller().Send()
	} else {
		// Start HLS segment recording immediately for preloaded streams
		if rawQuery == "" {
			rawQuery = "video&audio"
		}
		query, err := url.ParseQuery(rawQuery)
		if err == nil {
			preloadsMu.Lock()
			cons := preloads[stream]
			preloadsMu.Unlock()
			if cons != nil {
				// Get the stream name to pass to HLS recording
				streamName := GetStreamName(stream)
				go startHLSPreloadRecording(streamName, cons, query)
			}
		}
	}
}

func AddPreload(stream *Stream, rawQuery string) error {
	if rawQuery == "" {
		rawQuery = "video&audio"
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return err
	}

	preloadsMu.Lock()
	defer preloadsMu.Unlock()

	if cons := preloads[stream]; cons != nil {
		stream.RemoveConsumer(cons)
	}

	cons := probe.Create("preload", query)

	if err = stream.AddConsumer(cons); err != nil {
		return err
	}

	preloads[stream] = cons
	return nil
}

func DelPreload(stream *Stream) error {
	preloadsMu.Lock()
	defer preloadsMu.Unlock()

	if cons := preloads[stream]; cons != nil {
		stream.RemoveConsumer(cons)
		delete(preloads, stream)
		return nil
	}

	return errors.New("streams: preload not found")
}

func HasPreload(stream *Stream) bool {
	preloadsMu.Lock()
	defer preloadsMu.Unlock()

	_, ok := preloads[stream]
	return ok
}




// startHLSPreloadRecording starts the HLS segment recording for a preloaded stream
func startHLSPreloadRecording(src string, cons *probe.Probe, query url.Values) {
	// This will be called from the HLS package
	// We need to import this in a way that doesn't create circular dependencies
	// For now, we'll use a callback mechanism
	if hlsPreloadCallback != nil {
		hlsPreloadCallback(src, cons, query)
	}
}

// Callback function to be set by the HLS package
var hlsPreloadCallback func(src string, cons *probe.Probe, query url.Values)

// SetHLSPreloadCallback sets the callback function for HLS preload recording
func SetHLSPreloadCallback(callback func(src string, cons *probe.Probe, query url.Values)) {
	hlsPreloadCallback = callback
}
