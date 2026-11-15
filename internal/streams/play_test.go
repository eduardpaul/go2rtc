package streams

import (
	"errors"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

type playConn struct {
	medias  []*core.Media
	stopped bool
}

func (c *playConn) GetMedias() []*core.Media { return c.medias }

func (c *playConn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return core.NewReceiver(media, codec), nil
}

func (c *playConn) AddTrack(*core.Media, *core.Codec, *core.Receiver) error { return nil }

func (c *playConn) Start() error { return nil }

func (c *playConn) Stop() error {
	c.stopped = true
	return nil
}

func TestPlayStopDstOnSrcError(t *testing.T) {
	var dst *playConn
	HandleFunc("playdst", func(string) (core.Producer, error) {
		dst = &playConn{}
		return dst, nil
	})
	HandleFunc("playbad", func(string) (core.Producer, error) {
		return nil, errors.New("bad source")
	})

	stream, err := New("play_src_error", "playdst:")
	require.NoError(t, err)
	t.Cleanup(func() { Delete("play_src_error") })

	require.Error(t, stream.Play("playbad:"))
	require.NotNil(t, dst)
	require.True(t, dst.stopped)
}

func TestPlayStopSrcWithoutConsumer(t *testing.T) {
	var dst, src *playConn
	HandleFunc("playdst", func(string) (core.Producer, error) {
		dst = &playConn{} // without backchannel
		return dst, nil
	})
	HandleFunc("playsrc", func(string) (core.Producer, error) {
		src = &playConn{medias: []*core.Media{{
			Kind:      core.KindAudio,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{{Name: core.CodecPCMA, ClockRate: 8000}},
		}}}
		return src, nil
	})

	stream, err := New("play_no_consumer", "playdst:")
	require.NoError(t, err)
	t.Cleanup(func() { Delete("play_no_consumer") })

	require.Error(t, stream.Play("playsrc:"))
	require.True(t, dst.stopped)
	require.NotNil(t, src)
	require.True(t, src.stopped)
}

func TestPlayRouteSurvivesConsumerLeaving(t *testing.T) {
	HandleFunc("playdst", func(string) (core.Producer, error) {
		return &playConn{}, nil
	})

	stream, err := New("play_route_survives", "playdst:")
	require.NoError(t, err)
	t.Cleanup(func() { Delete("play_route_survives") })

	route := &playConn{}
	stream.AddInternalProducer(route)

	// A viewer (e.g. a snapshot request) comes and goes.
	viewer := &playConn{}
	stream.AddInternalConsumer(viewer)
	stream.RemoveConsumer(viewer)

	require.False(t, route.stopped)
	require.Len(t, stream.producers, 2)

	// When the route ends, Play's goroutine removes it from the list.
	stream.RemoveProducer(route)
	require.Len(t, stream.producers, 1)
}
