package main

import (
    "fmt"
    "time"
   // "github.com/maximecaron/DistributedTransactions/service"
    "github.com/maximecaron/DistributedTransactions/roundRegister"
    "math/rand"
)


func main() {
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
    var locked = false

    go  func () {
        for { 
            fl1.WithLease(func (timeout <- chan time.Time) {
                select {
                    case <- timeout:
                    default:
                      if (locked) {
                        panic("already locked")
                      }
                      locked = true
                      fmt.Printf("P1 got the lock \n")
                      time.Sleep(time.Millisecond*100)
                      locked = false
                }
            })
            time.Sleep(time.Duration(rand.Int31n(500)) * time.Millisecond)
            
        }
    }()

    
    go  func () {
          for { 
            fl2.WithLease(func (timeout <- chan time.Time) {
                select {
                    case <- timeout:
                    default:
                      if (locked) {
                        panic("already locked")
                      }
                      locked = true
                      fmt.Printf("P2 got the lock \n")
                      time.Sleep(time.Millisecond*100)
                      locked = false
                }
            })
            time.Sleep(time.Duration(rand.Int31n(500)) * time.Millisecond)
            
        }
    }()
    
    go  func () {
        for { 
            fl3.WithLease(func (timeout <- chan time.Time) {
                select {
                    case <- timeout:
                    default:
                      if (locked) {
                        panic("already locked")
                      }
                      locked = true
                      fmt.Printf("P3 got the lock \n")
                      time.Sleep(time.Millisecond*100)
                      locked = false
                }
            })
            time.Sleep(time.Duration(rand.Int31n(500)) * time.Millisecond)
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