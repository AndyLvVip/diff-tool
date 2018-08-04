package main

import (
	_ "bankdiff/conf"
	"bankdiff/service"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
	"time"
)

func main() {
	now := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go service.NewBigSmallBankService().Process(now, wg)
	go service.NewSuperBankService().Process(now, wg)
	wg.Wait()
	log.Printf("total time: %fs", time.Now().Sub(now).Seconds())

}
