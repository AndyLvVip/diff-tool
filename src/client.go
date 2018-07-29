package main

import (
	"base"
	"bufio"
	"fetcher"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"os"
	"service"
	"time"
)

func LoadBigSmallBank() {
	file, err := os.Open(fetcher.BigSmallBank.FileName)
	defer file.Close()
	base.CheckErr(err)
	reader := bufio.NewReader(transform.NewReader(file, simplifiedchinese.GBK.NewDecoder()))
	service.LoadBigSmallBank(reader)
}

func LoadSuperBank() {
	file, err := os.Open(fetcher.SuperBank.FileName)
	defer file.Close()
	base.CheckErr(err)
	reader := bufio.NewReader(transform.NewReader(file, simplifiedchinese.GBK.NewDecoder()))
	service.LoadSuperBank(reader)
}

func main() {
	fetcher.Download(fetcher.BigSmallBank)
	fetcher.Download(fetcher.SuperBank)

	LoadBigSmallBank()
	LoadSuperBank()

	now := time.Now()
	service.QueryAndGenerate4BigSmallBank(now)
	service.QueryAndGenerate4SuperBank(now)
}
