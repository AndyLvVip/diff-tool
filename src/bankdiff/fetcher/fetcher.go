package fetcher

import (
	"bankdiff/helper"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"time"
)

type BankInfo struct {
	Url      string
	fileName string
}

func (b *BankInfo) FilePathAndName(now time.Time) string {
	return fmt.Sprintf("result/%s/%s", helper.Format2yyyy_MM_dd(now), b.fileName)
}

var BigSmallBank = &BankInfo{"https://ebank.cgbchina.com.cn/corporbank/superEbankNoDownload.jsp?pms=true", "BigSmallBank.txt"}
var SuperBank = &BankInfo{"https://ebank.cgbchina.com.cn/corporbank/superEbankNoDownload.jsp", "SuperBank.txt"}

func Download(now time.Time, bankInfo *BankInfo) {
	log.Printf("downloading the file: %s\n", bankInfo.FilePathAndName(now))
	resp, err := http.Get(bankInfo.Url)
	if nil != err {
		helper.CheckErr(err)
	}
	if resp.StatusCode != 200 {
		helper.CheckErr(fmt.Errorf("response status is: %d", resp.StatusCode))
	}

	data := make([]byte, 1024)
	err = os.MkdirAll(path.Dir(bankInfo.FilePathAndName(now)), os.ModePerm)
	helper.CheckErr(err)
	file, err := os.Create(bankInfo.FilePathAndName(now))
	helper.CheckErr(err)
	defer file.Close()
	for {
		n, err := resp.Body.Read(data)
		if nil != err {
			if io.EOF == err {
				if n > 0 {
					file.Write(data[:n])
				}
				break
			}
			helper.CheckErr(err)
		}

		file.Write(data[:n])
	}
	resp.Body.Close()
	log.Printf("downloaded the file: %s\n", bankInfo.FilePathAndName(now))
}
