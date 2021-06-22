package helper

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/boundary/api/targets"
	"github.com/hashicorp/boundary/globals"
	targetspb "github.com/hashicorp/boundary/internal/gen/controller/api/resources/targets"
	"github.com/hashicorp/boundary/internal/proxy"
	"github.com/hashicorp/boundary/internal/session"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/helper/base62"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wspb"
)

const (
	DefaultGracePeriod  = time.Second * 15
	testSendRecvSendMax = uint32(DefaultGracePeriod * 3 / time.Second)
)

// TestSession represents an authorized session.
type TestSession struct {
	sessionId       string
	workerAddr      string
	transport       *http.Transport
	tofuToken       string
	connectionsLeft int32
	logger          hclog.Logger
}

// NewTestSession authorizes a session and creates all of the data
// necessary to initialize.
func NewTestSession(
	ctx context.Context,
	t *testing.T,
	logger hclog.Logger,
	tcl *targets.Client,
	targetId string,
) *TestSession {
	require := require.New(t)
	sar, err := tcl.AuthorizeSession(ctx, "ttcp_1234567890")
	require.NoError(err)
	require.NotNil(sar)

	s := &TestSession{
		sessionId: sar.Item.SessionId,
		logger:    logger,
	}
	authzString := sar.GetItem().(*targets.SessionAuthorization).AuthorizationToken
	marshaled, err := base58.FastBase58Decoding(authzString)
	require.NoError(err)
	require.NotZero(marshaled)

	sessionAuthzData := new(targetspb.SessionAuthorizationData)
	err = proto.Unmarshal(marshaled, sessionAuthzData)
	require.NoError(err)
	require.NotZero(sessionAuthzData.GetWorkerInfo())

	s.workerAddr = sessionAuthzData.GetWorkerInfo()[0].GetAddress()

	parsedCert, err := x509.ParseCertificate(sessionAuthzData.Certificate)
	require.NoError(err)
	require.Len(parsedCert.DNSNames, 1)

	certPool := x509.NewCertPool()
	certPool.AddCert(parsedCert)
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{sessionAuthzData.Certificate},
				PrivateKey:  ed25519.PrivateKey(sessionAuthzData.PrivateKey),
				Leaf:        parsedCert,
			},
		},
		RootCAs:    certPool,
		ServerName: parsedCert.DNSNames[0],
		MinVersion: tls.VersionTLS13,
	}

	s.transport = cleanhttp.DefaultTransport()
	s.transport.DisableKeepAlives = false
	s.transport.TLSClientConfig = tlsConf
	s.transport.IdleConnTimeout = 0

	return s
}

// connect returns a connected websocket for the stored session,
// connecting to the stored workerAddr with the configured transport.
//
// The returned (wrapped) net.Conn should be ready for communication.
func (s *TestSession) connect(ctx context.Context, t *testing.T) net.Conn {
	require := require.New(t)
	conn, resp, err := websocket.Dial(
		ctx,
		fmt.Sprintf("wss://%s/v1/proxy", s.workerAddr),
		&websocket.DialOptions{
			HTTPClient: &http.Client{
				Transport: s.transport,
			},
			Subprotocols: []string{globals.TcpProxyV1},
		},
	)
	require.NoError(err)
	require.NotNil(conn)
	require.NotNil(resp)
	require.Equal(resp.Header.Get("Sec-WebSocket-Protocol"), globals.TcpProxyV1)

	// Send the handshake.
	if s.tofuToken == "" {
		s.tofuToken, err = base62.Random(20)
	}

	require.NoError(err)
	handshake := proxy.ClientHandshake{TofuToken: s.tofuToken}
	err = wspb.Write(ctx, conn, &handshake)
	require.NoError(err)

	// Receive/check the handshake
	var handshakeResult proxy.HandshakeResult
	err = wspb.Read(ctx, conn, &handshakeResult)
	require.NoError(err)

	// This is just a cursory check to make sure that the handshake is
	// populated. We could check connections remaining too, but that
	// could legitimately be a trivial (zero) value.
	require.NotNil(handshakeResult.GetExpiration())
	s.connectionsLeft = handshakeResult.GetConnectionsLeft()

	return websocket.NetConn(ctx, conn, websocket.MessageBinary)
}

// TestNoConnectionsLeft asserts that there are no connections left.
func (s *TestSession) TestNoConnectionsLeft(t *testing.T) {
	require.Zero(t, s.connectionsLeft)
}

// ExpectConnectionStateOnController waits for all connections on
// the session to transition to the closed state on the controller.
func (s *TestSession) ExpectConnectionStateOnController(
	ctx context.Context,
	t *testing.T,
	sessionRepo *session.Repository,
	expectState session.ConnectionStatus,
) {
	require := require.New(t)
	// This is just for initialization of the actual state set.
	const sessionStatusUnknown session.ConnectionStatus = "unknown"

	// This currently needs to be at least 1m to deal with overlap on
	// the scheduler's default job interval.
	ctx, cancel := context.WithTimeout(ctx, time.Second*90)
	defer cancel()

	// Get all connections for the session on the controller.
	conns, err := sessionRepo.ListConnectionsBySessionId(ctx, s.sessionId)
	require.NoError(err)
	if len(conns) < 1 {
		s.logger.Warn("no connections returned for session, nothing to do")
		return
	}

	// Set up the waitgroups.
	var wg sync.WaitGroup
	wg.Add(len(conns))
	// Make a set of states, 1 per connection
	actualStates := make([]session.ConnectionStatus, len(conns))
	for i := range actualStates {
		actualStates[i] = sessionStatusUnknown
	}

	// Make expect set for comparison
	expectStates := make([]session.ConnectionStatus, len(conns))
	for i := range expectStates {
		expectStates[i] = expectState
	}

	for i, conn := range conns {
		go func(i int, conn *session.Connection) {
			for {
				if ctx.Err() != nil {
					break
				}
				_, states, err := sessionRepo.LookupConnection(ctx, conn.PublicId, nil)
				require.NoError(err)
				// Look at the first state in the returned list, which will
				// be the most recent state.
				actualStates[i] = states[0].Status
				if states[0].Status == session.StatusClosed {
					break
				}

				// Sleep 1s before checking again.
				time.Sleep(time.Second)
			}

			wg.Done()
		}(i, conn)
	}

	// Wait for all connections to be found.
	wg.Wait()

	// Assert
	require.Equal(expectStates, actualStates)
	s.logger.Debug("successfully asserted all connection states on controller", "expected_states", expectStates, "actual_states", actualStates)
}

// testSessionConnection abstracts a connected session.
type TestSessionConnection struct {
	conn   net.Conn
	logger hclog.Logger
}

// Connect returns a testSessionConnection for a testSession. Check
// the unexported connect method for the lower-level details.
func (s *TestSession) Connect(
	ctx context.Context,
	t *testing.T, // Just to add cleanup
) *TestSessionConnection {
	require := require.New(t)
	conn := s.connect(ctx, t)
	require.NotNil(conn)
	t.Cleanup(func() {
		conn.Close()
	})
	require.NotNil(conn)

	return &TestSessionConnection{
		conn:   conn,
		logger: s.logger,
	}
}

// testSendRecv runs a basic send/receive test over the returned
// connection, and returns whether or not all "pings" made it
// through.
//
// The test is a simple sequence number, ticking up every second to
// max. The passed in conn is expected to copy whatever it is
// received.
func (c *TestSessionConnection) testSendRecv(t *testing.T) bool {
	require := require.New(t)
	for i := uint32(0); i < testSendRecvSendMax; i++ {
		// Shuttle over the sequence number as base64.
		err := binary.Write(c.conn, binary.LittleEndian, i)
		if err != nil {
			c.logger.Debug("received error during write", "err", err)
			if errors.Is(err, net.ErrClosed) ||
				errors.Is(err, io.EOF) ||
				errors.Is(err, websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "timed out"}) {
				return false
			}

			require.FailNow(err.Error())
		}

		// Read it back
		var j uint32
		err = binary.Read(c.conn, binary.LittleEndian, &j)
		if err != nil {
			c.logger.Debug("received error during read", "err", err, "num_successfully_sent", i)
			if errors.Is(err, net.ErrClosed) ||
				errors.Is(err, io.EOF) ||
				errors.Is(err, websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "timed out"}) {
				return false
			}

			require.FailNow(err.Error())
		}

		require.Equal(j, i)

		// Sleep 1s
		time.Sleep(time.Second)
	}

	c.logger.Debug("finished send/recv successfully", "num_successfully_sent", testSendRecvSendMax)
	return true
}

// TestSendRecvAll asserts that we were able to send/recv all pings
// over the test connection.
func (c *TestSessionConnection) TestSendRecvAll(t *testing.T) {
	require.True(t, c.testSendRecv(t))
	c.logger.Debug("successfully asserted send/recv as passing")
}

// TestSendRecvFail asserts that we were able to send/recv all pings
// over the test connection.
func (c *TestSessionConnection) TestSendRecvFail(t *testing.T) {
	require.False(t, c.testSendRecv(t))
	c.logger.Debug("successfully asserted send/recv as failing")
}

// TestTcpServer allows for the creation of a very simple loopback TCP server
// for testing.
type TestTcpServer struct {
	logger hclog.Logger
	ln     net.Listener
	conns  map[string]net.Conn
}

// Port returns the port number for the testing server.
func (ts *TestTcpServer) Port() uint32 {
	_, portS, err := net.SplitHostPort(ts.ln.Addr().String())
	if err != nil {
		panic(err)
	}

	if portS == "" {
		panic("empty port in what should be TCP listener")
	}

	port, err := strconv.Atoi(portS)
	if err != nil {
		panic(err)
	}

	if port < 1 {
		panic("zero or negative port in what should be a TCP listener")
	}

	return uint32(port)
}

// Close closes the test server listener and all of its connections.
func (ts *TestTcpServer) Close() {
	ts.ln.Close()
	for _, conn := range ts.conns {
		conn.Close()
	}
}

func (ts *TestTcpServer) run() {
	for {
		conn, err := ts.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				ts.logger.Error("Accept() error in testTcpServer", "err", err)
			}

			return
		}

		ts.conns[conn.RemoteAddr().String()] = conn

		go func(c net.Conn) {
			io.Copy(c, c)
			c.Close()
		}(conn)
	}
}

// NewTestTcpServer creates and starts a TestTcpServer.
//
// The server listens on the default address. Use Port to get the selected port
// number.
func NewTestTcpServer(t *testing.T, logger hclog.Logger) *TestTcpServer {
	require := require.New(t)
	ts := &TestTcpServer{
		logger: logger,
		conns:  make(map[string]net.Conn),
	}
	var err error
	ts.ln, err = net.Listen("tcp", ":0")
	require.NoError(err)

	go ts.run()
	return ts
}
