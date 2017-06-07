package roundRegister

import (
	"context"
	"fmt"
	"time"
)

// Ballot number include proposer id make ballot unique
type Ballot struct {
	proposerID int
	// todo: use global time for proposalNo
	proposalNo time.Time
}

func (b *Ballot) After(other Ballot) bool {
	if b.proposalNo.After(other.proposalNo) {
		return true
	} else if other.proposalNo.After(b.proposalNo) {
		return false
	} else {
		return (b.proposerID > other.proposerID)
	}
}

func (b *Ballot) Equal(other Ballot) bool {
	return (b.proposerID == other.proposerID) && (b.proposalNo == other.proposalNo)
}

func (b *Ballot) String() string {
	return fmt.Sprintf("{proposalNo:%s, proposerID:%d}", b.proposalNo, b.proposerID)
}

func NewBallot(proposerID int, proposalNo time.Time) Ballot {
	return Ballot{
		proposerID: proposerID,
		proposalNo: proposalNo,
	}
}

type roundRegisterResponseType int

const (
	nackREADType roundRegisterResponseType = iota
	ackREADType
	nackWRITEType
	ackWriteType
)

type roundRegisterCommandType int

const (
	readType roundRegisterCommandType = iota
	writeType
)

type roundRegisterResponse struct {
	responseType roundRegisterResponseType
	k            Ballot
	write        Ballot
	v            interface{}
}

type roundRegisterCommand struct {
	CommandType roundRegisterCommandType
	K           Ballot
	V           interface{}
}

// R. Boichat, P. Dutta, S. Frolund, and R. Guerraoui, “Deconstructing paxos,” SIGACT News, vol. 34, no. 1,
type RoundRegister struct {
	bootStrap time.Duration
	read      Ballot // highest read ballot accepted (paxos proposal)
	write     Ballot // highest write ballot accepted (paxos accepted)
	value     interface{}
	//The number of peers must be known in advance and must not increase during runtime,
	peers     []string
	transport Transport
}

func NewRoundRegister(tr Transport, peersList []string, bootStrapDuration time.Duration) *RoundRegister {
	// To enable crash-recovery model without Persistent State:
	// we init read and write ballot to current time
	// this ensure that the register will abort a read or write request
	// with ballot smaller than the ballot seen before crash
	r := &RoundRegister{
		read:      NewBallot(0, time.Now()),
		write:     NewBallot(0, time.Now()),
		value:     nil,
		transport: tr,
		peers:     peersList,
		bootStrap: bootStrapDuration,
	}
	go r.HandleRequests()
	return r
}

func (r *RoundRegister) SendRead(ctx context.Context, target string, k Ballot) (error, *roundRegisterResponse) {
	cmd := &roundRegisterCommand{}
	cmd.CommandType = readType
	cmd.K = k
	resp, err := r.transport.makeRPC(ctx, target, cmd)
	if err != nil {
		return err, nil
	}
	roundRegisterResp := resp.Response.(*roundRegisterResponse)
	return nil, roundRegisterResp
}

func (r *RoundRegister) SendWrite(ctx context.Context, target string, k Ballot, v interface{}) (error, *roundRegisterResponse) {
	cmd := &roundRegisterCommand{}
	cmd.CommandType = writeType
	cmd.K = k
	cmd.V = v
	resp, err := r.transport.makeRPC(ctx, target, cmd)
	if err != nil {
		return err, nil
	}
	roundRegisterResp := resp.Response.(*roundRegisterResponse)
	return nil, roundRegisterResp
}

// Read value using Round k
func (r *RoundRegister) Read(ctx context.Context, k Ballot) (error, interface{}) {
	// Create a response channel
	respCh := make(chan *roundRegisterResponse, len(r.peers))
	quorumCtx, cancel := context.WithCancel(ctx)
	defer cancel() // after scope make sure the goroutine operation are cancelled
	peerRead := func(peer string, k Ballot) {
		err, resp := r.SendRead(quorumCtx, peer, k)
		if err == nil {
			select {
			case <-quorumCtx.Done():
			case respCh <- resp:
			}
		}
	}
	var maxWriteTime Ballot
	var maxVal interface{}

	for _, element := range r.peers {
		go peerRead(element, k)
	}

	responseReceived := 0
	responseNeeded := r.quorumSize()
	// wait for (n+1)/2 response
	for responseReceived < responseNeeded {
		select {
		case response := <-respCh:
			if response.responseType == nackREADType {
				return fmt.Errorf("read abort"), nil
			}
			// Return the element with the highest write version number
			if response.write.After(maxWriteTime) {
				maxWriteTime = response.write
				maxVal = response.v
			}
			responseReceived++
		case <-time.After(200 * time.Millisecond):
			return fmt.Errorf("timeout"), nil
		}
	}

	return nil, maxVal
}

func (r *RoundRegister) quorumSize() int {
	return (len(r.peers) / 2) + 1
}

// Write value using round k
func (r *RoundRegister) Write(ctx context.Context, k Ballot, value interface{}) error {
	quorumCtx, cancel := context.WithCancel(ctx)
	defer cancel() // after scope make sure the goroutine operation are cancelled

	// Create a response channel
	respCh := make(chan *roundRegisterResponse, len(r.peers))
	peerWrite := func(peer string) {
		err, resp := r.SendWrite(quorumCtx, peer, k, value)
		if err == nil {
			select {
			case <-quorumCtx.Done():
			case respCh <- resp:
			}
		}
	}
	for _, element := range r.peers {
		go peerWrite(element)
	}
	responseReceived := 0
	responseNeeded := r.quorumSize()
	// wait for (n+1)/2 response
	for responseReceived < responseNeeded {
		select {
		case resp := <-respCh:
			if resp.responseType == nackWRITEType {
				// if received at least one nack abort
				return fmt.Errorf("write abort")
			}
			responseReceived++
		case <-time.After(50 * time.Millisecond):
			return fmt.Errorf("timeout")
		}
	}
	// Received ack from (n+1)/2 node, Commit
	return nil

}

func (r *RoundRegister) handleRead(req *roundRegisterCommand) (*roundRegisterResponse, error) {
	resp := &roundRegisterResponse{}
	// If a read or write was already received with ballot larger or equal to the request  abort
	if !req.K.After(r.read) || !req.K.After(r.write) {
		resp.responseType = nackREADType
		resp.k = req.K
	} else {
		r.read = req.K
		resp.responseType = ackREADType
		resp.k = req.K
		// return ballot of last accepted value
		resp.write = r.write
		resp.v = r.value
	}
	return resp, nil
}

func (r *RoundRegister) handleWrite(req *roundRegisterCommand) (*roundRegisterResponse, error) {
	resp := &roundRegisterResponse{}
	// If a read or write was already received with ballot larger to the request abort
	if r.read.After(req.K) || r.write.After(req.K) {
		resp.responseType = nackWRITEType
		resp.k = req.K
	} else {
		r.write = req.K
		r.value = req.V
		resp.responseType = ackWriteType
		resp.k = req.K
	}
	return resp, nil
}

func (r *RoundRegister) HandleRequest(req *roundRegisterCommand) (*roundRegisterResponse, error) {
	resp := &roundRegisterResponse{}
	switch req.CommandType {
	case readType:
		return r.handleRead(req)
	case writeType:
		return r.handleWrite(req)
	default:
		return resp, fmt.Errorf("unknow command")
	}

}

// Listen for request in loop
func (r *RoundRegister) HandleRequests() {
	// To enable crash-recovery model  without Persistent State:
	// we guarantee that any lease which was in the register when the
	// process crashed has timed out by sleeping (tmax) before accepting request
	time.Sleep(r.bootStrap)
	for {
		select {
		case rpc := <-r.transport.Consumer():
			// Verify the command
			req := rpc.Command.(*roundRegisterCommand)
			resp, err := r.HandleRequest(req)
			select {
			case rpc.RespChan <- RPCResponse{
				Response: resp,
				Error:    err,
			}:
			case <-time.After(50 * time.Millisecond):
			}
			// todo case <- r.stop:
		}
	}
}
