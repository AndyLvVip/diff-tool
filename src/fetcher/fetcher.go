package fetcher

import (
	"net/http"
	"os"
	"fmt"
	"io"
	"base"
)

type BankInfo struct {
	Url      string
	FileName string
}

var BigSmallBank = &BankInfo{"https://ebank.cgbchina.com.cn/corporbank/superEbankNoDownload.jsp?pms=true", "BigSmallBank.txt"}
var SuperBank = &BankInfo{"https://ebank.cgbchina.com.cn/corporbank/superEbankNoDownload.jsp", "SuperBank.txt"}

func Download(bankInfo *BankInfo) {
	resp, err := http.Get(bankInfo.Url)
	if nil != err || 200 != resp.StatusCode {
		panic(err)

	}

	data := make([]byte, 1024)
	file , err := os.Create(bankInfo.FileName)
	base.CheckErr(err)
	defer file.Close()
	for  {
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
	fmt.Printf("downloaded the file: %s\n", bankInfo.FileName)
}
