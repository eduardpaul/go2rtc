package homekit

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/camera"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// dbg* counters limit the volume of backchannel diagnostic logging so we can
// compare the doorbell's real ELD RTP framing (recv) against what we transmit
// (send) without flooding the container logs.
var dbgRecvCount, dbgSendCount int

func dbgLogRTP(dir string, p *rtp.Packet, count *int) {
	if *count >= 15 {
		return
	}
	head := p.Payload
	if len(head) > 8 {
		head = head[:8]
	}
	log.Printf("[hk-dbg] %s seq=%d ts=%d pt=%d marker=%t ver=%d len=%d head=%x",
		dir, p.SequenceNumber, p.Timestamp, p.PayloadType, p.Marker, p.Version, len(p.Payload), head)
	*count++
}

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

	// audioSend/audioSeq assign strictly increasing RTP timestamp/sequence
	// for the backchannel audio SSRC across separate AddTrack calls (one per
	// talkback playback) and across depacketized AUs within a call. Must not
	// reset per-call: RTP requires monotonic sequence/timestamp for a given
	// SSRC or receivers may treat the stream as stale and drop it.
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
				dbgLogRTP("recv-audio", packet, &dbgRecvCount)
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
// Combines PR #1981's AAC-ELD codec detection (so the HomeKit audio session is
// negotiated as ELD/16000) with single-AU packetization that mirrors the
// doorbell's OWN mic stream: exactly one AU per RTP packet, timestamp +480
// (30ms @ 16kHz), marker=false, paced at 30ms. Our upstream ffmpeg RTSP
// producer bundles ~14 AUs per MPEG4-GENERIC packet, which the Aqara G4 will
// not render; we must split those bundles back to one AU per packet.
func (c *Client) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	const sampleRate = 16000
	// The eld ffmpeg template now uses -frame_length 480 (libfdk's granule-length
	// knob; the generic -frame_size is ignored, see internal/ffmpeg/ffmpeg.go), so
	// libfdk_aac emits true 480-sample (30ms) ELD frames. The RTP timestamp step
	// MUST match the real frame size: 480 keeps the clock at true 16kHz AND matches
	// the RTPTime=30 we advertise (helpers.go trackToAudio) so the doorbell frames
	// each AU correctly. (History: while the encoder wrongly emitted 512-sample
	// frames, this was 512 to keep the clock right — T10. Fixing the encoder to a
	// true 480 lets us restore 480 here and end the 480-advertised/512-actual
	// mismatch that degraded quality — Q1.)
	const sampleSize = 480 // 30ms @ 16kHz — real libfdk_aac ELD frame length (-frame_length 480)

	switch codec.Name {
	case core.CodecELD, core.CodecOpus:
		sender := core.NewSender(media, track.Codec)

		log.Printf("[hk-dbg] AddTrack codec=%s trackCodec=%s isRTP=%t pt=%d",
			codec.Name, track.Codec.Name, track.Codec.IsRTP(), track.Codec.PayloadType)

		var nextSend time.Time
		var started bool
		// Per-AU send spacing. ffmpeg packs ~14 AAC-ELD AUs into each RTP packet;
		// RTPDepay splits them and they arrive here as a rapid burst. Sending the
		// whole burst instantly overruns the doorbell's jitter buffer (cuts),
		// while spacing them at the full 32ms realtime rate adds ~450ms standing
		// latency. Spread each burst at TALKBACK_PACE_MS (default 12ms): fast
		// enough to keep latency low, gentle enough to avoid buffer overrun.
		// Non-accumulating: nextSend resets to now after each idle gap, so a
		// startup burst cannot build permanent standing latency.
		paceMS := 12
		if v := os.Getenv("TALKBACK_PACE_MS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				paceMS = n
			}
		}
		paceInterval := time.Duration(paceMS) * time.Millisecond

		writeRTP := func(packet *rtp.Packet) {
			if dbgSendCount < 5 {
				log.Printf("[hk-dbg] writeRTP entry len=%d sessionNil=%t remoteNil=%t",
					len(packet.Payload), c.audioSession == nil,
					c.audioSession != nil && c.audioSession.Remote == nil)
			}
			if c.audioSession == nil || c.audioSession.Remote == nil {
				return
			}

			// Talkspurt start = the very first packet of the stream. The ffmpeg
			// source is continuous (encodes silence too), so bundle-arrival gaps
			// are NOT talkspurts and must not re-trigger the marker.
			talkspurtStart := !started
			started = true

			if paceInterval > 0 {
				now := time.Now()
				if now.Before(nextSend) {
					time.Sleep(nextSend.Sub(now))
					now = nextSend
				}
				nextSend = now.Add(paceInterval)
			}

			// Wrap the raw AU as a single-AU MPEG4-GENERIC payload:
			// [AU-headers-length=16][AU-size<<3][AU bytes].
			auSize := uint16(len(packet.Payload))
			wrapped := make([]byte, 4+auSize)
			wrapped[1] = 16
			binary.BigEndian.PutUint16(wrapped[2:], auSize<<3)
			copy(wrapped[4:], packet.Payload)
			packet.Payload = wrapped

			// Flag the first packet of each talkspurt so the doorbell's audio
			// jitter buffer opens/resets playback (RFC 3550 §5.1).
			packet.Marker = talkspurtStart
			packet.Timestamp = c.audioSend
			packet.SequenceNumber = c.audioSeq
			c.audioSend += sampleSize
			c.audioSeq++

			dbgLogRTP("send-audio", packet, &dbgSendCount)

			if n, err := c.audioSession.WriteRTP(packet); err == nil {
				c.Send += n
			}
		}

		// Seed the RTP sequence/timestamp once so the stream does not look
		// stale on a reused SSRC across successive talkback playbacks.
		if c.audioSeq == 0 && c.audioSend == 0 {
			c.audioSeq = uint16(rand.Uint32())
			c.audioSend = rand.Uint32()
		}

		if track.Codec.IsRTP() {
			// Split bundled MPEG4-GENERIC packets into individual AUs.
			depay := aac.RTPDepay(writeRTP)
			// TEMP measurement: count AUs per incoming RTP packet and the
			// wall-clock gap between incoming packets. This tells us whether
			// ffmpeg is bundling many AUs into one RTP packet (bursty arrival)
			// or delivering ~1 AU per packet at realtime.
			var measPrev time.Time
			var measCount int
			sender.Handler = func(pkt *rtp.Packet) {
				if measCount < 20 {
					now := time.Now()
					var gapMS int64
					if !measPrev.IsZero() {
						gapMS = now.Sub(measPrev).Milliseconds()
					}
					measPrev = now
					// MPEG4-GENERIC: 2-byte AU-headers-length, then N*2-byte AU headers.
					aus := 0
					if len(pkt.Payload) >= 2 {
						hdrBits := int(pkt.Payload[0])<<8 | int(pkt.Payload[1])
						aus = (hdrBits / 16) // each AU header = 16 bits
					}
					log.Printf("[hk-dbg] recv-eld payloadLen=%d aus=%d gapMS=%d", len(pkt.Payload), aus, gapMS)
					measCount++
				}
				depay(pkt)
			}
		} else {
			sender.Handler = writeRTP
		}

		sender.HandleRTP(track)
		c.Senders = append(c.Senders, sender)
	}

	return nil
}
