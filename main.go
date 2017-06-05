package main

import (
    "fmt"
"time"
"runtime"
   // "github.com/maximecaron/DistributedTransactions/service"
    "github.com/maximecaron/DistributedTransactions/roundRegister"
    "math/rand"
)

func main() {
    runtime.GOMAXPROCS(6)
    fmt.Printf("program start\n")
    
    // Create 3 simulated transport connected together
    addr1,t1 :=  roundRegister.NewInmemTransport()
    addr2,t2 :=  roundRegister.NewInmemTransport()
    addr3,t3 :=  roundRegister.NewInmemTransport()
    t1.Connect(addr1, t1)
    t1.Connect(addr2, t2)
    t1.Connect(addr3, t3)
    t2.Connect(addr1, t1)
    t2.Connect(addr2, t2)
    t2.Connect(addr3, t3)
    t3.Connect(addr1, t1)
    t3.Connect(addr2, t2)
    t3.Connect(addr3, t3)
    peerlist := []string{addr1, addr2,addr3}
    
    fl1 := roundRegister.NewFlease("p1",t1,peerlist)
    fl2 := roundRegister.NewFlease("p2",t2,peerlist)
    fl3 := roundRegister.NewFlease("p3",t3,peerlist)
    i :=0 
    go  func () {
        for { 
            fl1.WithLease(func (timeout <- chan time.Time) {
                fmt.Printf("P1 got lease\n")
                // call fl1.GetLease() to renew
                i++
                // Wait for lease timeout
                <- timeout
                fmt.Printf("P1 lease timeout\n")
            })
            time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
            
        }
    }()

    
    go  func () {
          for { 
            fl2.WithLease(func (timeout <- chan time.Time) {
                fmt.Printf("P2 got lease\n") 
                // call fl2.GetLease() to renew
                i++
                // Wait for lease timeout
                <- timeout
                fmt.Printf("P2 lease timeout\n")
            })
            time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
            
        }
    }()
    
    go  func () {
        for { 
            fl3.WithLease(func (timeout <- chan time.Time) {
                fmt.Printf("P3 got lease\n") 
                // call fl3.GetLease() to renew
                i++
                //Wait for lease timeout
                <- timeout
                fmt.Printf("P3 lease timeout\n")
            })
            time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
        }
    }()
  

    for {
        time.Sleep(time.Second)
    }


   /* 
    s := service.New("localhost:1100","test")
    if err := s.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}
    */
	//fmt.Printf("fin\n")
}