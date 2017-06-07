package roundRegister

import (
	"context"
	"fmt"
	"time"
)

// https://github.com/xtreemfs/xtreemfs/blob/10369f6973271ddc09a2f21e3cc494ca22926526/java/xtreemfs-flease/src/main/java/org/xtreemfs/foundation/flease/acceptor/FleaseAcceptor.java

type Lease struct {
	Timeout     time.Time // lease expiration time
	LeaseHolder int       // Process holding the lease
}

// Flease is a lease process which is fault-tolerant
// and consider processes that recover after a crash.
type Flease struct {
	register *RoundRegister // pa
	tmax     time.Duration  // maximum time span of lease validity
	// epsilon is maximum clock time difference allowed between any 2 process
	// Should be max_clock_drift*2
	epsilon time.Duration
	// roundTimeout_ms time the proposer waits for an answer
	roundTimeout time.Duration
	p            int // local process
}

func NewFlease(p int, tr Transport, peersList []string) *Flease {
	return NewFleaseWithEps(p, tr, peersList, time.Millisecond*4, time.Millisecond*17)
}

func NewFleaseWithEps(p int, tr Transport, peersList []string, eps time.Duration, tmax time.Duration) *Flease {
	// For the system to make progress, we require we require (tmax > eps)

	// wait (tmax+eps) before RoundRegister can respond to request
	// while waiting, participates in Paxos as a non-voting member
	//  does not respond with promise or acknowledgment messages
	rr := NewRoundRegister(tr, peersList, tmax+eps)
	flease := &Flease{
		p:        p,
		register: rr,
		epsilon:  eps,
		tmax:     tmax,
	}
	return flease
}

func (fl *Flease) IsLeaseValid(l *Lease) bool {
	return l != nil && l.Timeout.After(time.Now())

}

func (fl *Flease) IsHoldingLease(l *Lease) bool {
	return fl.IsLeaseValid(l) && l.LeaseHolder == fl.p
}

func (fl *Flease) renewLease(ctx context.Context, lease *Lease) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			remainingTime := lease.Timeout.Sub(time.Now())
			time.Sleep(remainingTime / 2)
			lease, _ = fl.GetLease()
		}
	}
}

func (fl *Flease) WithLease(fn func(<-chan time.Time)) (time.Time, error) {
	for {
		lease, err := fl.GetLease()
		if err != nil {
			return time.Now(), err
		}
		if fl.IsHoldingLease(lease) {
			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel() // after scope make sure the goroutine operation are cancelled
			// Start goroutine to renew lease
			go fl.renewLease(ctx, lease)

			fn(time.After(lease.Timeout.Sub(time.Now())))
			return lease.Timeout, nil
		} else {
			// sleep before retry to avoid wasting time reading latest lease
			time.Sleep(lease.Timeout.Sub(time.Now()))
		}
	}
}

func (fl *Flease) GetLease() (*Lease, error) {
	for {
		lease, err := fl.TryGetLease()
		if err != nil {
			// Consensu iteration failed
			//fmt.Printf("%s %s\n",fl.p, err.Error())
		} else if lease == nil {
			fmt.Printf("lease is nil")
		} else {
			return lease, nil
		}
	}
}

func (fl *Flease) TryGetLease() (*Lease, error) {
	var l *Lease = nil

	now := time.Now()

	readErr, val := fl.register.Read(context.TODO(), NewBallot(fl.p, now))

	if readErr == nil {
		// Sucessful read from majority of Nodes
		if val != nil {
			// if lease is not empty
			l = val.(*Lease)
		}

		if l != nil && now.After(l.Timeout) && l.Timeout.Add(fl.epsilon).After(now) {
			// Lease is not empty but
			// since l.Timeout + epsilon is > now
			// its possible the process holding the lease still think its still hold it
			// we wait for maximum clock drift time and retry
			time.Sleep(fl.epsilon)
			return nil, fmt.Errorf("now is between lease.timeout  and lease.timeout+eps")
		}

		if !fl.IsLeaseValid(l) {
			// lease is empty or expired, trying to acquire it
			l = &Lease{
				LeaseHolder: fl.p,
				Timeout:     now.Add(fl.tmax),
			}
		} else if l.LeaseHolder == fl.p {
			// We are already holding the lease but need to renew it
			l = &Lease{
				LeaseHolder: fl.p,
				Timeout:     now.Add(fl.tmax),
			}
		}
		// current process must always ensure that any lease it returns has been
		// successfully written to the register, regardless of whether it own it
		// because writes can be incomplete
		writeErr := fl.register.Write(context.TODO(), NewBallot(fl.p, now), l)
		if writeErr == nil {
			return l, nil
		}
		return nil, writeErr
	}
	return nil, readErr
}
