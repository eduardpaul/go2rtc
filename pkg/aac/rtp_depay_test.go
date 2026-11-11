package aac

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/pion/rtp"
)

// payload builds an AAC-hbr payload: one AU-header per AU, with the given
// declared sizes, followed by data.
func payload(sizes []int, data []byte) []byte {
	b := make([]byte, 2+2*len(sizes))
	binary.BigEndian.PutUint16(b, uint16(16*len(sizes)))
	for i, n := range sizes {
		binary.BigEndian.PutUint16(b[2+2*i:], uint16(n)<<3)
	}
	return append(b, data...)
}

func TestRTPDepayFragmented(t *testing.T) {
	small := bytes.Repeat([]byte{1}, 90)
	big := make([]byte, 300)
	for i := range big {
		big[i] = byte(i)
	}
	pair := append(bytes.Repeat([]byte{3}, 40), bytes.Repeat([]byte{4}, 50)...)

	var got [][]byte
	depay := RTPDepay(func(p *rtp.Packet) { got = append(got, append([]byte(nil), p.Payload...)) })

	depay(&rtp.Packet{Header: rtp.Header{Timestamp: 0}, Payload: payload([]int{90}, small)})
	// 300-byte AU split into 138-byte fragments (ffmpeg rtpenc_aac layout)
	for i := 0; i < len(big); i += 138 {
		end := min(i+138, len(big))
		depay(&rtp.Packet{Header: rtp.Header{Timestamp: 480, Marker: end == len(big)}, Payload: payload([]int{300}, big[i:end])})
	}
	// two AUs bundled in one packet still work
	depay(&rtp.Packet{Header: rtp.Header{Timestamp: 960}, Payload: payload([]int{40, 50}, pair)})
	// a dangling first fragment followed by a new timestamp is discarded
	depay(&rtp.Packet{Header: rtp.Header{Timestamp: 1440}, Payload: payload([]int{300}, big[:138])})
	depay(&rtp.Packet{Header: rtp.Header{Timestamp: 1920}, Payload: payload([]int{90}, small)})

	want := [][]byte{small, big, pair[:40], pair[40:], small}
	if len(got) != len(want) {
		t.Fatalf("got %d AUs, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("AU %d: got %d bytes, want %d", i, len(got[i]), len(want[i]))
		}
	}
}
