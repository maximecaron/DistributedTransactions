package main

import (
    "fmt"
"time"
"runtime"
   // "github.com/maximecaron/DistributedTransactions/service"
    "github.com/maximecaron/DistributedTransactions/roundRegister"
)

func main() {
    runtime.GOMAXPROCS(6)
    
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
      
    go  func () {
        leader := false
        for { 
          lease, err := fl1.GetLease();
          if (err !=nil) {
             // fmt.Printf("P1 %s",err.Error())
          } else if (lease == nil){
              fmt.Printf("P1 lease was nil\n")
          } else if (fl1.IsHoldingLease(lease)){
              if (leader == false) {
                 fmt.Printf("P1 got lease\n") 
                 leader = true
              }
          } else {
              leader = false
              fmt.Printf("P1: lease is held by %s\n",lease.P)
              time.Sleep(lease.Timeout.Sub(time.Now()))
          }
        }

    }()
    
    go  func () {
        leader := false
        for { 
          lease, err := fl2.GetLease();
          if (err !=nil) {
             // fmt.Printf("P2 %s",err.Error())
          } else if (lease == nil){
              fmt.Printf("P2 lease was nil\n")
          } else if (fl2.IsHoldingLease(lease)){
              if (leader == false) {
                 fmt.Printf("P2 got lease\n") 
                 leader = true
              }
          } else {
              leader = false;
              fmt.Printf("P2: lease is held by %s\n",lease.P)
              time.Sleep(lease.Timeout.Sub(time.Now()))
          }
        }
    }()
    
    go  func () {
        leader := false
        for { 
          lease, err := fl3.GetLease();
          if (err !=nil) {
           //   fmt.Printf("P3 %s",err.Error())
          } else if (lease == nil){
              fmt.Printf("P3 lease was nil\n")
          } else if (fl3.IsHoldingLease(lease)){
              if (leader == false) {
                 fmt.Printf("P3 got lease\n") 
                 leader = true
              }
          } else {
              leader = false;
              fmt.Printf("P3: lease is held by %s\n",lease.P)
              time.Sleep(lease.Timeout.Sub(time.Now()))
          }
        }
    }()
    
    
    
   
    for {
        time.Sleep(time.Microsecond)
    }


   /* 
    s := service.New("localhost:1100","test")
    if err := s.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}
    */
	//fmt.Printf("fin\n")
}