package homekit

import (
	"bufio"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/stretchr/testify/require"
)

type proxyServer struct{}

func (proxyServer) GetPair(string) []byte        { return nil }
func (proxyServer) AddPair(string, []byte, byte) {}
func (proxyServer) DelPair(string)               {}
func (proxyServer) AddConn(any)                  {}
func (proxyServer) DelConn(any)                  {}

// hapPipe returns two sides of secured HAP conn over TCP
func hapPipe(t *testing.T) (client, server *hap.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	conn1, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	conn2, err := ln.Accept()
	require.NoError(t, err)

	key := make([]byte, 32)

	client, err = hap.NewConn(conn1, bufio.NewReadWriter(bufio.NewReader(conn1), bufio.NewWriter(conn1)), key, true)
	require.NoError(t, err)
	server, err = hap.NewConn(conn2, bufio.NewReadWriter(bufio.NewReader(conn2), bufio.NewWriter(conn2)), key, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn1.Close()
		_ = conn2.Close()
	})

	return
}

func waitClosed(t *testing.T, conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := conn.Read(make([]byte, 0x1000))
	require.Error(t, err)
	if ne, ok := err.(net.Error); ok {
		require.False(t, ne.Timeout(), "conn not closed")
	}
}

func waitReturn(t *testing.T, ch chan error) {
	select {
	case <-ch:
	case <-time.After(time.Second):
		require.FailNow(t, "handler didn't return")
	}
}

func TestProxyControllerClose(t *testing.T) {
	iphone, con := hapPipe(t)
	acc, camera := hapPipe(t)

	handler := ProxyHandler(proxyServer{}, acc)

	ch := make(chan error)
	go func() { ch <- handler(con) }()

	// controller (ex. iPhone) disconnects
	_ = iphone.Close()
	waitReturn(t, ch)

	// accessory (ex. Camera) conn should be closed
	waitClosed(t, camera)
}

func TestProxyAccessoryClose(t *testing.T) {
	iphone, con := hapPipe(t)
	acc, camera := hapPipe(t)

	handler := ProxyHandler(proxyServer{}, acc)

	ch := make(chan error)
	go func() { ch <- handler(con) }()

	// accessory (ex. Camera) disconnects
	_ = camera.Close()
	waitReturn(t, ch)

	// controller (ex. iPhone) conn should be closed
	waitClosed(t, iphone)
}

func TestProxyHandleAccExit(t *testing.T) {
	_, con := hapPipe(t)
	acc, camera := hapPipe(t)

	p := &Proxy{
		con:  con,
		acc:  acc,
		res:  make(chan *http.Response),
		done: make(chan struct{}),
	}

	ch := make(chan error)
	go func() { ch <- p.handleAcc() }()

	// response without request, nobody waits for it
	_, err := camera.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// controller handler is finished
	close(p.done)
	waitReturn(t, ch)
}
