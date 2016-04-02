package roundRegister
import (
  "time"
  "sync"
  "fmt"
  crand "crypto/rand"
)
// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}
	RespChan chan<- RPCResponse
}

type Transport interface {
   Consumer() <-chan RPC
   makeRPC(target string, command interface{}) (rpcResp RPCResponse, err error)
}


// Implement Transport interface using Channel 
type InmemTransport struct {
    sync.RWMutex
	consumerCh chan RPC
	localAddr  string
    peers      map[string]*InmemTransport
    timeout    time.Duration
}

func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}


// NewInmemTransport is used to initialize a new transport
// and generates a random local address.
func NewInmemTransport() (string, *InmemTransport) {
	addr := generateUUID()
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr:  addr,
		peers:      make(map[string]*InmemTransport),
		timeout:    50 * time.Millisecond,
	}
	return addr, trans
}

// Consumer implements the Transport interface.
func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

func (i *InmemTransport) makeRPC(target string, command interface{}) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := make(chan RPCResponse)
	peer.consumerCh <- RPC{
		Command:  command,
		RespChan: respCh,
	}

	// Wait for a response
	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	}
	return
}

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (i *InmemTransport) Connect(peer string, trans *InmemTransport) {
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}
