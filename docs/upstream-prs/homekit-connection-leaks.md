# Upstream PRs: HomeKit connection leaks

Two independent PRs against `AlexxIT/go2rtc` `master`. Each branch is based on
upstream `master` and is on the fork (`eduardpaul/go2rtc`).

---

## PR 1: `fix/homekit-connection-leaks`

### Title

Fix HomeKit connection leaks and hangs

### Description

A HomeKit camera has only a few HAP sessions and stream slots. go2rtc could leak HAP
connections to an accessory until the accessory's HAP server stopped answering. Once
that happened, new connections hung forever and the leak grew faster.

**1. `hap.Client.Dial` / `Pair`: no deadline, no close on error** (`pkg/hap`)

- After `net.DialTimeout`, the pair-verify handshake had no deadline. A device that
  accepts TCP but never answers blocked the caller forever and kept the socket open.
- On any handshake error, the conn was never closed (there was a `// TODO: close conn on error`).
- `Pair` (pair-setup) had the same pattern.

Fix:

- Each pair-verify request gets the existing, unused `ConnDeadline` (3s). The deadline
  is removed after a successful verify, so the persistent connection is unchanged.
- Pair-setup gets a whole-handshake deadline (`PairSetupDeadline`, 30s), because SRP can
  be slow on low-power accessories.
- The conn is closed on every error path.
- The handshake moved into an unexported `pairVerify(conn)`, so it can be tested without
  mDNS. The diff stays small.
- `apiPair` closes the pair-setup conn, which can't be used for requests after pairing.

**2. HomeKit proxy: accessory conn never closed** (`pkg/homekit/proxy.go`)

- For each controller connection, `internal/homekit` dials the real accessory and calls
  `ProxyHandler(srv, client.Conn)`. When the controller disconnected, only the controller
  conn was closed. So every controller reconnect (e.g. an iPhone waking up) left one
  more HAP session open on the accessory.
- `handleAcc` could block forever on the unbuffered `p.res <- res` after `handleCon` had
  returned.
- If the accessory conn broke, `handleCon` waited forever for a response.

Fix:

- `ProxyHandler` closes the accessory conn when the handler ends.
- `handleAcc` stops on a `done` channel.
- When the accessory conn breaks, `handleAcc` closes `p.res` and the controller conn.
  The controller reconnects and gets a fresh accessory session.

**3. HomeKit producer: `GetMedias` could hang forever** (`pkg/homekit/producer.go`)

- `streams.Producer` calls `GetMedias` while holding its mutex. If the device stopped
  answering after pair-verify, `GetFirstAccessory` blocked forever and the stream could
  no longer be used or stopped.
- Fix: a `core.ConnDeadline` deadline only around this one request, removed right after
  it. Nothing else reads the HAP conn at that point.
- I didn't add a generic per-request deadline to `hap.Client.Do`. It would interact with
  pair-setup timing, slow snapshot (`/resource`) responses and the events reader.

### Tests

- `pkg/hap/client_test.go`:
  - A fake device accepts TCP and never answers: `pairVerify` fails within about
    `ConnDeadline`, and the device sees the conn closed.
  - A wrong response to pair-verify or pair-setup also closes the conn.
- `pkg/homekit/proxy_test.go`:
  - The controller disconnects: the handler returns and the accessory conn is closed.
  - The accessory disconnects: the handler returns and the controller conn is closed.
  - `handleAcc` exits when a response is pending and the handler has finished.
- The new tests fail on current `master` (where they compile) and pass with this PR.
- `go vet` is clean on the changed packages. With the PR, the full `go test ./...` has
  the same set of failing tests as on `master`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

## PR 2: `fix/streams-play-leaks`

### Title

Fix producer leaks in Stream.Play on errors

### Description

`Stream.Play` (`POST /api/streams?dst=...&src=...`, also used by the ffmpeg and wyoming
modules) opens new connections on two error paths and never closes them:

1. **The new `src` fails.** When the stream's producer has no active connection, `Play`
   opens a new `dst` client to the device. If `GetProducer(source)` then fails, the
   function returned without stopping `dst`. A client that keeps re-posting a source
   that fails left one more open session on the device for each request. For HomeKit
   cameras, which have very few HAP sessions, this eventually made the accessory stop
   answering.
2. **No consumer found.** If no producer can consume the source, the function returned
   `can't find consumer` without stopping the `src` it had created (for example, a
   leftover `exec`/`ffmpeg` process or an open RTSP connection).

Fix:

- Stop `dst` before returning the source error.
- Before returning `can't find consumer`, stop `src`, but only if `Play` created it
  from a URL. A `core.Producer` passed in by the caller is left alone.
- No other behaviour changes.

### Tests

`internal/streams/play_test.go` adds fake producers for both paths:

- `TestPlayStopDstOnSrcError`
- `TestPlayStopSrcWithoutConsumer`

Both fail on current `master` and pass with this PR.

`TestRecursion` and `TestTempate` in this package already fail on `master`
(`streams: source not supported`), and this PR doesn't change them. With the PR, the
full `go test ./...` has the same set of failing tests as on `master`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
