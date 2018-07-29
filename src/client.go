package main

import (
	_ "conf"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"service"
	"sync"
	"time"
)

func main() {
	now := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go service.ProcessBigSmallBank(now, wg)
	go service.Process4SuperBank(now, wg)
	wg.Wait()
	log.Printf("total time: %fs", time.Now().Sub(now).Seconds())

}
