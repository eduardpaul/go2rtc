package hap

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// listen starts a fake device. Handler gets each accepted conn and
// the returned channel is closed when the client closes the conn.
func listen(t *testing.T, handler func(conn net.Conn)) (string, chan struct{}) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	closed := make(chan struct{})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		if handler != nil {
			handler(conn)
		}

		// wait until client closes the conn
		_, _ = io.Copy(io.Discard, conn)
		close(closed)
	}()

	return ln.Addr().String(), closed
}

func waitClosed(t *testing.T, closed chan struct{}) {
	select {
	case <-closed:
	case <-time.After(time.Second):
		require.FailNow(t, "client didn't close conn")
	}
}

func TestPairVerifyTimeout(t *testing.T) {
	// device accepts TCP and never answers
	addr, closed := listen(t, nil)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	c := &Client{ClientID: GenerateUUID(), ClientPrivate: GenerateKey()}

	ts := time.Now()
	err = c.pairVerify(conn)
	require.Error(t, err)
	require.Less(t, time.Since(ts), ConnDeadline+time.Second)

	waitClosed(t, closed)
}

func TestPairVerifyError(t *testing.T) {
	// device answers with wrong response
	addr, closed := listen(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	})

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	c := &Client{ClientID: GenerateUUID(), ClientPrivate: GenerateKey()}
	require.Error(t, c.pairVerify(conn))

	waitClosed(t, closed)
}

func TestPairError(t *testing.T) {
	// device answers with wrong response
	addr, closed := listen(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	})

	c := &Client{DeviceAddress: addr, ClientID: GenerateUUID(), ClientPrivate: GenerateKey()}
	require.Error(t, c.Pair("", "123-45-679"))

	waitClosed(t, closed)
}
