package fetcher

import (
	"base"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"log"
)

type BankInfo struct {
	Url      string
	FileName string
}

var BigSmallBank = &BankInfo{"https://ebank.cgbchina.com.cn/corporbank/superEbankNoDownload.jsp?pms=true", "result/BigSmallBank.txt"}
var SuperBank = &BankInfo{"https://ebank.cgbchina.com.cn/corporbank/superEbankNoDownload.jsp", "result/SuperBank.txt"}

func Download(bankInfo *BankInfo) {
	log.Printf("downloading the file: %s\n", bankInfo.FileName)
	resp, err := http.Get(bankInfo.Url)
	if nil != err {
		base.CheckErr(err)
	}
	if resp.StatusCode != 200 {
		base.CheckErr(fmt.Errorf("response status is: %d", resp.StatusCode))
	}

	data := make([]byte, 1024)
	err = os.MkdirAll(path.Dir(bankInfo.FileName), os.ModePerm)
	base.CheckErr(err)
	file, err := os.Create(bankInfo.FileName)
	base.CheckErr(err)
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
			base.CheckErr(err)
		}

		file.Write(data[:n])
	}
	resp.Body.Close()
	log.Printf("downloaded the file: %s\n", bankInfo.FileName)
}
