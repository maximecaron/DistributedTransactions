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

func NewRoundRegister(tr Transport, peersList []string, bootStrap time.Duration) (*RoundRegister) {
	r := &RoundRegister{
		read: time.Now(),
        write: time.Now(),
        value: nil,
        transport: tr,
		peers:      peersList,
        bootStrap: bootStrap,
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
    respArray := []*roundRegisterResponse  {}
    var maxWriteTime time.Time
    var maxVal interface{}
    // wait for (n+1)/2 response
    for _ , element := range r.peers {
        //TODO handle RPC timeout
        err, resp := r.SendRead(element, k)
        if (err != nil){
            return err, nil
        }
        respArray = append(respArray, resp)
    }
    
    // Return the element with the highest write version number 
    // from a majority of peers
    for _ , element := range respArray {
        if (element.responseType== nackREADType){
            return fmt.Errorf("abort"), nil 
        } 
        if (element.write.After(maxWriteTime)) {
                maxWriteTime = element.write
                maxVal = element.v
        } 
    }
    return nil, maxVal
}

// Write value using round k
func (r *RoundRegister) Write(k time.Time, value interface{}) error {
    // wait for (n+1)/2 response
    for _ , element := range r.peers {
        //TODO handle RPC timeout
        err, resp  := r.SendWrite(element, k, value)
        if (err !=nil){ 
           return err
        }
        if (resp.responseType == nackWRITEType){
            // if received at least one nack abort
            return fmt.Errorf("abort")
        }
        
    }
    // Received ack from (n+1)/2 node, Commit
    return nil
    
}


func (r *RoundRegister) HandleRequest(req *roundRegisterCommand) (*roundRegisterResponse, error){
 resp := &roundRegisterResponse {}
 // Handle read request
 if (req.CommandType == readType) {
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
 // Handle write request
 if (req.CommandType == writeType) {
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
 return resp, fmt.Errorf("unknow command")
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