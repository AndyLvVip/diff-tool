package service

import (
	"bankdiff/base"
	"bankdiff/conf"
	"bankdiff/dao"
	"bankdiff/fetcher"
	"bankdiff/model"
	"bufio"
	"database/sql"
	"fmt"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io"
	"log"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

func ProcessBigSmallBank(now time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	fetcher.Download(now, fetcher.BigSmallBank)

	file, err := os.Open(fetcher.BigSmallBank.FilePathAndName(now))
	defer file.Close()
	base.CheckErr(err)
	reader := bufio.NewReader(transform.NewReader(file, simplifiedchinese.GBK.NewDecoder()))
	LoadBigSmallBank(reader)

	QueryAndGenerate4BigSmallBank(now)
}

func LoadBigSmallBank(reader *bufio.Reader) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	base.CheckErr(err)
	defer db.Close()

	_, err = reader.ReadString('\n') //discard the first line
	base.CheckErr(err)

	dao.TruncateBigSmallBank(db)

	var bsbSlices []*model.BigSmallBankModel
	now := time.Now()
	for {
		line, err := reader.ReadString('\n')
		if nil != err {
			if io.EOF != err {
				base.CheckErr(err)
			} else {
				if len(line) > 0 {
					bsbSlices = append(bsbSlices, model.ToBigSmallBank(strings.TrimSpace(line)))
				}
				break
			}
		}
		bsbSlices = append(bsbSlices, model.ToBigSmallBank(strings.TrimSpace(line)))
	}
	log.Printf("loaded file time: %f\n", time.Now().Sub(now).Seconds())
	count := int(math.Ceil(float64(1.0*len(bsbSlices)) / 10000))
	wg := &sync.WaitGroup{}
	wg.Add(count)
	now = time.Now()
	for i := 0; i < count; i++ {
		if (i+1)*10000 < len(bsbSlices) {
			go dao.BatchInsert4BigSmallBank(wg, bsbSlices[i*10000:(i+1)*10000], db)
		} else {
			go dao.BatchInsert4BigSmallBank(wg, bsbSlices[i*10000:], db)
		}
	}
	wg.Wait()
	log.Printf("inserted time: %f\n", time.Now().Sub(now).Seconds())
}

func QueryAndGenerate4BigSmallBank(now time.Time) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	base.CheckErr(err)
	defer db.Close()
	added := dao.FetchAddedBigSmallBank(db)
	updated := dao.FetchUpdatedBigSmallBank(db)
	deleted := dao.FetchDeletedBigSmallBank(db)

	GenerateDiffFileSql4BigSmallBank(now, added, updated, deleted)

	model.PayeeCheckSql4BigSmallBank(now, updated, deleted)
}

func GenerateDiffFileSql4BigSmallBank(now time.Time, added []*model.BigSmallBankModel, updated []*model.BigSmallBankModel, deleted []*model.BigSmallBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/patch/big_small_%s.sql", base.Format2yyyy_MM_dd(now), base.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	base.CheckErr(err)
	file, err := os.Create(filePathAndName)
	base.CheckErr(err)
	defer file.Close()

	if len(added) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 新增的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range added {

			file.WriteString(bsd.AddedSqlScript() + "\n")
		}
	}

	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range updated {
			file.WriteString(bsd.UpdatedSqlScript() + "\n")
		}
	}

	if len(deleted) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range deleted {
			file.WriteString(bsd.DeletedSqlScript() + "\n")
		}
	}
	log.Println("generated " + filePathAndName)
}
