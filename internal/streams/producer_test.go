package streams

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

type routeConn struct {
	stopped bool
}

func (c *routeConn) GetMedias() []*core.Media { return nil }

func (c *routeConn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return core.NewReceiver(media, codec), nil
}

func (c *routeConn) AddTrack(*core.Media, *core.Codec, *core.Receiver) error { return nil }

func (c *routeConn) Start() error { return nil }

func (c *routeConn) Stop() error {
	c.stopped = true
	return nil
}

func TestPlayRouteSurvivesConsumerLeaving(t *testing.T) {
	HandleFunc("routedst", func(string) (core.Producer, error) {
		return &routeConn{}, nil
	})

	stream, err := New("play_route_survives", "routedst:")
	require.NoError(t, err)
	t.Cleanup(func() { Delete("play_route_survives") })

	route := &routeConn{}
	stream.AddInternalProducer(route)

	// A viewer (e.g. a snapshot request) comes and goes.
	viewer := &routeConn{}
	stream.AddInternalConsumer(viewer)
	stream.RemoveConsumer(viewer)

	require.False(t, route.stopped)
	require.Len(t, stream.producers, 2)

	// When the route ends, Play's goroutine removes it from the list.
	stream.RemoveProducer(route)
	require.Len(t, stream.producers, 1)
}
