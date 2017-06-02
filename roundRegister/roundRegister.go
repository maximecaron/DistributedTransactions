package roundRegister
import (
  "time"
  "fmt"
)

type roundRegisterResponseType int

const (
    nackREADType roundRegisterResponseType  = iota
    ackREADType
    nackWRITEType
    ackWriteType
    bootStraping
)
type roundRegisterCommandType int
const (
    readType roundRegisterCommandType = iota
    writeType
)
type roundRegisterResponse struct {
    responseType roundRegisterResponseType
    k time.Time
    write time.Time
    v interface{}
}

type roundRegisterCommand struct {
    CommandType roundRegisterCommandType 
    K time.Time
    V interface{}
}

type RoundRegister struct {
    bootStrap time.Duration
	read time.Time
    write time.Time
    value interface{}
    //The number of peers must be known in advance and must not increase during runtime,
    peers []string 
    transport Transport
}

func NewRoundRegister(tr Transport, peersList []string, bootStrapDuration time.Duration) (*RoundRegister) {
	r := &RoundRegister{
		read: time.Now(),
        write: time.Now(),
        value: nil,
        transport: tr,
		peers:      peersList,
        bootStrap: bootStrapDuration,        
	}
    go r.HandleRequests()
	return r
}

func (r *RoundRegister) SendRead(target string, k time.Time) (error, *roundRegisterResponse) {
    cmd := &roundRegisterCommand{}
    cmd.CommandType = readType
    cmd.K = k
    resp, err := r.transport.makeRPC(target,cmd)
    if (err != nil){
        return err, nil
    }
    roundRegisterResp := resp.Response.(*roundRegisterResponse)
    return nil, roundRegisterResp
}

func (r *RoundRegister) SendWrite(target string, k time.Time, v interface{}) (error, *roundRegisterResponse) {
    cmd := &roundRegisterCommand{}
    cmd.CommandType = writeType
    cmd.K = k
    cmd.V = v
    resp, err := r.transport.makeRPC(target,cmd)
    if (err != nil){
        return err, nil
    }
    roundRegisterResp := resp.Response.(*roundRegisterResponse)
    return nil, roundRegisterResp
}

// Read value using Round k
func (r *RoundRegister) Read(k time.Time) (error, interface{}) {
    // Create a response channel
    respCh := make(chan *roundRegisterResponse, len(r.peers))
    peerRead := func(peer string, k time.Time) {
            err, resp := r.SendRead(peer, k)
            if (err == nil){
                respCh <- resp
            }
    }
    var maxWriteTime time.Time
    var maxVal interface{}
    
    for _ , element := range r.peers {
        go peerRead(element, k)
    }

    responseReceived := 0
    responseNeeded := r.quorumSize()
    // wait for (n+1)/2 response
    for responseReceived < responseNeeded {
        select {
        case response := <-respCh:
        if (response.responseType == nackREADType){
               return fmt.Errorf("abort"), nil 
        } 
        if (response.responseType != bootStraping) {
          // Return the element with the highest write version number 
          if (response.write.After(maxWriteTime)) {
                maxWriteTime = response.write
                maxVal = response.v
          } 
          responseReceived++
        }
        case <-time.After(50 * time.Millisecond):
            return fmt.Errorf("timeout"), nil
        }
    }

    return nil, maxVal
}

func (r *RoundRegister) quorumSize() int {
    return (len(r.peers) / 2) +1;
}

// Write value using round k
func (r *RoundRegister) Write(k time.Time, value interface{}) error {
    // Create a response channel
    respCh := make(chan *roundRegisterResponse, len(r.peers))
    peerWrite := func(peer string) {
        err, resp  := r.SendWrite(peer, k, value)
        if (err ==nil){ 
           respCh <- resp
        }
    }
    for _ , element := range r.peers {
        go peerWrite(element)
    }
    responseReceived := 0
    responseNeeded := r.quorumSize()
    // wait for (n+1)/2 response
    for responseReceived < responseNeeded {
        select {
        case resp := <-respCh:
          if (resp.responseType != bootStraping) {
            if (resp.responseType == nackWRITEType){
              // if received at least one nack abort
              return fmt.Errorf("abort")
            }
            responseReceived++
          }
        case <-time.After(50 * time.Millisecond):
          return fmt.Errorf("timeout")
       }
    }
    // Received ack from (n+1)/2 node, Commit
    return nil
    
}

func (r *RoundRegister) handleRead(req *roundRegisterCommand) (*roundRegisterResponse, error){
  resp := &roundRegisterResponse {}
   if (r.write.After(req.K)  || r.read.After(req.K)) {
            resp.responseType = nackREADType
            resp.k = req.K
   } else {
            r.read = req.K
            resp.responseType = ackREADType
            resp.k = req.K
            resp.write = r.write
            resp.v = r.value
   }
   return resp, nil 
}

func (r *RoundRegister) handleWrite(req *roundRegisterCommand) (*roundRegisterResponse, error){
  resp := &roundRegisterResponse {}
  if (r.write.After(req.K) || r.read.After(req.K)) {
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

func (r *RoundRegister) HandleRequest(req *roundRegisterCommand) (*roundRegisterResponse, error){
 resp := &roundRegisterResponse {}
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
        time.Sleep(r.bootStrap)
		for {
			select {
			case rpc := <-r.transport.Consumer():
				// Verify the command
				req := rpc.Command.(*roundRegisterCommand)
                resp, err := r.HandleRequest(req)
                rpc.RespChan <- RPCResponse {
                    Response: resp,
                    Error: err,
                }	
			}
		}
}