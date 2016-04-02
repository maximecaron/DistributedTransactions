package roundRegister
import (
    "fmt"
    "time"
)
type Lease struct {
    Timeout time.Time // lease expiration time
    P string // Process holding the lease
}

// Flease is a lease process which is fault-tolerant 
// and consider processes that recover after a crash.
type Flease struct {
    register *RoundRegister // pa
    tmax time.Duration // maximum time span of lease validity
    epsilon time.Duration // maximum clock time difference
    p string // local process
}

func NewFlease(p string, tr Transport, peersList []string) (*Flease) {
    return NewFleaseWithEps(p,tr,peersList,time.Second, time.Second*14)
}

func NewFleaseWithEps(p string, tr Transport, peersList []string, eps time.Duration, tmax time.Duration) (*Flease) {
    // wait (tmax+eps) before RoundRegister can respond to request
    rr := NewRoundRegister(tr, peersList, tmax + eps)
    flease := &Flease{
        p: p,
        register: rr,
        epsilon: eps,
        tmax: tmax,
    }
    return flease
}

func (fl *Flease) IsHoldingLease(l *Lease) (bool) {
 return l!=nil && time.Now().Before(l.Timeout) && l.P == fl.p
}

func (fl *Flease) WithLease(fn func(<- chan time.Time)) time.Time {
    for { 
          lease, err := fl.GetLease();
          if (err !=nil) {
             // fmt.Printf("P1 %s",err.Error())
          } else if (lease == nil){
              fmt.Printf("P1 lease was nil\n")
          } else if (fl.IsHoldingLease(lease)) {
              fn(time.After(lease.Timeout.Sub(time.Now())))
              return lease.Timeout
          } else {
              time.Sleep(lease.Timeout.Sub(time.Now()))
          }
    }
}

func (fl *Flease) GetLease() (*Lease,error) {
    var l *Lease = nil
    now := time.Now()
   
    readErr, val := fl.register.Read(now)
    
    if (readErr == nil){       
        if (val != nil){
            l = val.(*Lease)
        }
        
        if (l !=nil && now.After(l.Timeout) && l.Timeout.Add(fl.epsilon).After(now) ){
            // since l.Timeout + epsilon is > now
            // its possible the process holding the lease still think its still hold it
            // we wait for maximum clock drift time and retry 
            time.Sleep(fl.epsilon)
            return fl.GetLease();
        }
        
        if (l == nil || now.After(l.Timeout) ){
           // lease is expired, trying to acquire it
           l = &Lease {
               P: fl.p,
               Timeout: now.Add(fl.tmax),
           } 
        } else if (l.P == fl.p) {
           // We are holding the lease but need to renew it
           l = &Lease {
               P: fl.p,
               Timeout: now.Add(fl.tmax),
           } 
        }
        
    } 
    
    // current process must always ensure that any lease it returns has been
    // successfully written to the register, regardless of whether the lease is owned by it
    // because writes can be incomplete
    writeErr := fl.register.Write(now, l)
    if (writeErr == nil){
        return l, nil
    }
    
    return nil, fmt.Errorf("abort")
}