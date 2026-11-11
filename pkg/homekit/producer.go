package homekit

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/camera"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// Deprecated: rename to Producer
type Client struct {
	core.Connection

	hap  *hap.Client
	srtp *srtp.Server

	videoSRTP *srtp.Server
	audioSRTP *srtp.Server

	videoConfig camera.SupportedVideoStreamConfiguration
	audioConfig camera.SupportedAudioStreamConfiguration

	videoSession *srtp.Session
	audioSession *srtp.Session

	// audioSend/audioSeq give the backchannel SSRC a strictly increasing RTP
	// timestamp/sequence across AddTrack calls (one per talkback route). They
	// must never reset, or the camera treats the stream as stale. audioMu
	// guards them because overlapping routes call AddTrack concurrently.
	audioMu   sync.Mutex
	audioSend uint32
	audioSeq  uint16

	stream *camera.Stream

	MaxWidth  int `json:"-"`
	MaxHeight int `json:"-"`
	Bitrate   int `json:"-"` // in bits/s
}

func Dial(rawURL string, server *srtp.Server) (*Client, error) {
	conn, err := hap.Dial(rawURL)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "homekit",
			Protocol:   "udp",
			RemoteAddr: conn.Conn.RemoteAddr().String(),
			Source:     rawURL,
			Transport:  conn,
		},
		hap:  conn,
		srtp: server,
	}

	return client, nil
}

func (c *Client) Conn() net.Conn {
	return c.hap.Conn
}

func (c *Client) GetMedias() []*core.Media {
	if c.Medias != nil {
		return c.Medias
	}

	acc, err := c.hap.GetFirstAccessory()
	if err != nil {
		return nil
	}

	char := acc.GetCharacter(camera.TypeSupportedVideoStreamConfiguration)
	if char == nil {
		return nil
	}
	if err = char.ReadTLV8(&c.videoConfig); err != nil {
		return nil
	}

	char = acc.GetCharacter(camera.TypeSupportedAudioStreamConfiguration)
	if char == nil {
		return nil
	}
	if err = char.ReadTLV8(&c.audioConfig); err != nil {
		return nil
	}

	c.SDP = fmt.Sprintf("%+v\n%+v", c.videoConfig, c.audioConfig)

	c.Medias = []*core.Media{
		videoToMedia(c.videoConfig.Codecs),
		audioToMedia(c.audioConfig.Codecs),
		{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs: []*core.Codec{
				{
					Name:        core.CodecJPEG,
					ClockRate:   90000,
					PayloadType: core.PayloadTypeRAW,
				},
			},
		},
	}

	// PR #1981: advertise a single clean AAC-ELD sendonly speaker codec.
	// A messy list (all recv codecs + a plain AAC) makes RTSP MatchMedia pick
	// the wrong codec/config for the ffmpeg talkback producer.
	c.Medias = append(c.Medias, &core.Media{
		Kind:      core.KindAudio,
		Direction: core.DirectionSendonly,
		Codecs: []*core.Codec{
			{
				Name:      core.CodecELD,
				ClockRate: 16000,
				Channels:  1,
			},
		},
	})

	return c.Medias
}

func (c *Client) Start() error {
	if c.Receivers == nil {
		return errors.New("producer without tracks")
	}

	if c.Receivers[0].Codec.Name == core.CodecJPEG {
		return c.startMJPEG()
	}

	videoTrack := c.trackByKind(core.KindVideo)
	videoCodec := trackToVideo(videoTrack, &c.videoConfig.Codecs[0], c.MaxWidth, c.MaxHeight)

	audioTrack := c.trackByKind(core.KindAudio)
	audioCodec := trackToAudio(audioTrack, &c.audioConfig.Codecs[0])

	c.videoSRTP = srtp.NewServer(":0")
	if err := c.videoSRTP.Start(); err != nil {
		return err
	}
	c.audioSRTP = srtp.NewServer(":0")
	if err := c.audioSRTP.Start(); err != nil {
		c.videoSRTP.Close()
		return err
	}

	c.videoSession = &srtp.Session{Local: c.srtpEndpoint(c.videoSRTP)}
	c.audioSession = &srtp.Session{Local: c.srtpEndpoint(c.audioSRTP)}

	var err error
	c.stream, err = camera.NewStream(c.hap, videoCodec, audioCodec, c.videoSession, c.audioSession, c.Bitrate)
	if err != nil {
		c.videoSRTP.Close()
		c.audioSRTP.Close()
		return err
	}

	c.videoSession.PayloadType = 99
	c.videoSession.RTCPInterval = 500 * time.Millisecond
	c.audioSession.PayloadType = 110
	c.audioSession.RTCPInterval = 5 * time.Second

	c.videoSRTP.AddSession(c.videoSession)
	c.audioSRTP.AddSession(c.audioSession)

	deadline := time.NewTimer(core.ConnDeadline)

	if videoTrack != nil {
		c.videoSession.OnReadRTP = func(packet *rtp.Packet) {
			deadline.Reset(core.ConnDeadline)
			videoTrack.WriteRTP(packet)
			c.Recv += len(packet.Payload)
		}

		if audioTrack != nil {
			c.audioSession.OnReadRTP = func(packet *rtp.Packet) {
				audioTrack.WriteRTP(packet)
				c.Recv += len(packet.Payload)
			}
		}
	} else {
		c.audioSession.OnReadRTP = func(packet *rtp.Packet) {
			deadline.Reset(core.ConnDeadline)
			audioTrack.WriteRTP(packet)
			c.Recv += len(packet.Payload)
		}
	}

	if c.audioSession.OnReadRTP != nil {
		c.audioSession.OnReadRTP = timekeeper(c.audioSession.OnReadRTP)
	}

	<-deadline.C

	return nil
}

func (c *Client) Stop() error {
	if c.videoSession != nil && c.videoSession.Remote != nil {
		c.videoSRTP.DelSession(c.videoSession)
	}
	if c.audioSession != nil && c.audioSession.Remote != nil {
		c.audioSRTP.DelSession(c.audioSession)
	}
	if c.videoSRTP != nil {
		c.videoSRTP.Close()
	}
	if c.audioSRTP != nil {
		c.audioSRTP.Close()
	}

	return c.Connection.Stop()
}

func (c *Client) trackByKind(kind string) *core.Receiver {
	for _, receiver := range c.Receivers {
		if receiver.Codec.Kind() == kind {
			return receiver
		}
	}
	return nil
}

func (c *Client) startMJPEG() error {
	receiver := c.Receivers[0]

	for {
		b, err := c.hap.GetImage(1920, 1080)
		if err != nil {
			return err
		}

		c.Recv += len(b)

		packet := &rtp.Packet{
			Header:  rtp.Header{Timestamp: core.Now90000()},
			Payload: b,
		}
		receiver.WriteRTP(packet)
	}
}

func (c *Client) srtpEndpoint(server *srtp.Server) *srtp.Endpoint {
	return &srtp.Endpoint{
		Addr:       c.hap.LocalIP(),
		Port:       uint16(server.Port()),
		MasterKey:  []byte(core.RandString(16, 0)),
		MasterSalt: []byte(core.RandString(14, 0)),
		SSRC:       rand.Uint32(),
	}
}

func timekeeper(handler core.HandlerFunc) core.HandlerFunc {
	const sampleRate = 16000
	const sampleSize = 480

	var send time.Duration
	var firstTime time.Time

	return func(packet *rtp.Packet) {
		now := time.Now()

		if send != 0 {
			elapsed := now.Sub(firstTime) * sampleRate / time.Second
			if send+sampleSize > elapsed {
				return // drop overflow frame
			}
		} else {
			firstTime = now
		}

		send += sampleSize

		packet.Timestamp = uint32(send)

		handler(packet)
	}
}

// AddTrack sends backchannel (talkback) audio to the camera.
//
// HomeKit doorbells such as the Aqara G4 only play AAC-ELD framed exactly like
// their own microphone stream: one AU per RTP packet wrapped in an RFC 3640
// AU-header, timestamp +480 per AU (30 ms @ 16 kHz, matching the advertised
// RTPTime=30), real-time cadence, and the marker bit on the first packet.
func (c *Client) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	if codec.Name != core.CodecELD {
		return nil // GetMedias only advertises an AAC-ELD sendonly track
	}

	const sampleSize = 480 // libfdk_aac ELD with -frame_length 480

	// Send pacing: spaces AUs that arrive bunched (e.g. ALSA capture periods)
	// so the doorbell's jitter buffer gets a steady flow. Non-accumulating:
	// it never builds standing latency after an idle gap.
	pace := 30 * time.Millisecond
	if v := os.Getenv("TALKBACK_PACE_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			pace = time.Duration(n) * time.Millisecond
		}
	}

	c.audioMu.Lock()
	if c.audioSeq == 0 && c.audioSend == 0 {
		c.audioSeq = uint16(rand.Uint32())
		c.audioSend = rand.Uint32()
	}
	c.audioMu.Unlock()

	var nextSend time.Time
	var started bool

	writeAU := func(packet *rtp.Packet) {
		if c.audioSession == nil || c.audioSession.Remote == nil {
			return
		}

		if pace > 0 {
			now := time.Now()
			if now.Before(nextSend) {
				time.Sleep(nextSend.Sub(now))
				now = nextSend
			}
			nextSend = now.Add(pace)
		}

		// single-AU MPEG4-GENERIC payload: [AU-headers-length=16][AU-size<<3][AU]
		payload := make([]byte, 4+len(packet.Payload))
		payload[1] = 16
		binary.BigEndian.PutUint16(payload[2:], uint16(len(packet.Payload))<<3)
		copy(payload[4:], packet.Payload)

		c.audioMu.Lock()
		out := &rtp.Packet{
			Header: rtp.Header{
				Marker:         !started, // talkspurt start (RFC 3550 §5.1)
				SequenceNumber: c.audioSeq,
				Timestamp:      c.audioSend,
			},
			Payload: payload,
		}
		c.audioSeq++
		c.audioSend += sampleSize
		c.audioMu.Unlock()
		started = true

		if n, err := c.audioSession.WriteRTP(out); err == nil {
			c.Send += n
		}
	}

	sender := core.NewSender(media, track.Codec)
	if track.Codec.IsRTP() {
		// sources (ffmpeg RTSP/RTP) may bundle several AUs per packet
		sender.Handler = aac.RTPDepay(writeAU)
	} else {
		sender.Handler = writeAU
	}
	sender.HandleRTP(track)
	c.Senders = append(c.Senders, sender)

	return nil
}
