package main

import (
	_ "bankdiff/conf"
	"bankdiff/service"
	"log"
	"sync"
	"time"
)

func main() {
	now := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go service.NewBaseBankService().Process(now, wg, service.NewBigSmallBankService())
	go service.NewBaseBankService().Process(now, wg, service.NewSuperBankService())
	wg.Wait()
	log.Printf("total time: %fs", time.Now().Sub(now).Seconds())
}
