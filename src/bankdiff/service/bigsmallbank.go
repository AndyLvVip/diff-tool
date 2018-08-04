package service

import (
	"bankdiff/conf"
	"bankdiff/dao"
	"bankdiff/fetcher"
	"bankdiff/helper"
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

type BigSmallBankService struct {
}

var bsb = BigSmallBankService{}

func NewBigSmallBankService() *BigSmallBankService {
	return &bsb
}

func (service *BigSmallBankService) Process(now time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	fetcher.Download(now, fetcher.BigSmallBank)

	file, err := os.Open(fetcher.BigSmallBank.FilePathAndName(now))
	helper.CheckErr(err)
	defer file.Close()
	reader := bufio.NewReader(transform.NewReader(file, simplifiedchinese.GBK.NewDecoder()))
	service.load(reader)

	service.queryAndGenerate(now)
}

func (*BigSmallBankService) load(reader *bufio.Reader) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	helper.CheckErr(err)
	defer db.Close()

	_, err = reader.ReadString('\n') //discard the first line
	helper.CheckErr(err)

	dao.NewBigSmallBankDao().Truncate(db)

	var bsbSlices []*model.BigSmallBankModel
	now := time.Now()
	for {
		line, err := reader.ReadString('\n')
		if nil != err {
			if io.EOF != err {
				helper.CheckErr(err)
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
			go dao.NewBigSmallBankDao().BatchInsert(wg, bsbSlices[i*10000:(i+1)*10000], db)
		} else {
			go dao.NewBigSmallBankDao().BatchInsert(wg, bsbSlices[i*10000:], db)
		}
	}
	wg.Wait()
	log.Printf("inserted time: %f\n", time.Now().Sub(now).Seconds())
}

func (service *BigSmallBankService) queryAndGenerate(now time.Time) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	helper.CheckErr(err)
	defer db.Close()
	added := dao.NewBigSmallBankDao().FetchAddedList(db)
	updated := dao.NewBigSmallBankDao().FetchUpdatedList(db)
	deleted := dao.NewBigSmallBankDao().FetchDeletedList(db)

	service.generateDiffFileSql(now, added, updated, deleted)

	service.payeeCheckSql(now, updated, deleted)
}

func (*BigSmallBankService) generateDiffFileSql(now time.Time, added []*model.BigSmallBankModel, updated []*model.BigSmallBankModel, deleted []*model.BigSmallBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/patch/big_small_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	helper.CheckErr(err)
	file, err := os.Create(filePathAndName)
	helper.CheckErr(err)
	defer file.Close()
	wh := &helper.WriteHelper{W: file}

	if len(added) > 0 {
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 新增的记录\n")
		wh.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range added {
			wh.WriteString(bsd.AddedSqlScript() + "\n")
		}
	}

	if len(updated) > 0 {
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 变更的记录\n")
		wh.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range updated {
			wh.WriteString(bsd.UpdatedSqlScript() + "\n")
		}
	}

	if len(deleted) > 0 {
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 删除的记录\n")
		wh.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range deleted {
			wh.WriteString(bsd.DeletedSqlScript() + "\n")
		}
	}

	if nil != wh.Err() {
		helper.CheckErr(wh.Err())
	}
	log.Println("generated " + filePathAndName)
}

func (service *BigSmallBankService) payeeCheckSql(now time.Time, updated []*model.BigSmallBankModel, deleted []*model.BigSmallBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/check/fin_payee_big_small_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	helper.CheckErr(err)
	file, err := os.Create(filePathAndName)
	helper.CheckErr(err)
	defer file.Close()
	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 普通到账（大小额支行联行号）\n")
		file.WriteString("-- 检测是否存在联行号有变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		service.writeCheckSql(file, updated)
	}
	if len(deleted) > 0 {
		file.WriteString("\n\n\n\n\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 普通到账（大小额支行联行号）\n")
		file.WriteString("-- 检测是否存在联行号有删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		service.writeCheckSql(file, deleted)
	}
	log.Println("generated " + filePathAndName)
}

func (service *BigSmallBankService) writeCheckSql(file *os.File, vals []*model.BigSmallBankModel) {
	file.WriteString("\n")
	file.WriteString("-- 检测广发提现账号\n")
	var args []string
	for _, bsb := range vals {
		args = append(args, helper.SqlValue(bsb.BankNo))
	}
	inBankNos := strings.Join(args, ", ")
	sql := fmt.Sprintf("SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 1 AND a.arrivalTimeType=0 AND a.branchBankNo IN (%s);",
		inBankNos,
	)
	file.WriteString(sql + "\n")

	file.WriteString("\n")
	file.WriteString("-- 检测收款方模板数据\n")
	sql = fmt.Sprintf("SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 1 AND a.arrivalTimeType=0 AND a.branchBankNo IN (%s);",
		inBankNos,
	)
	file.WriteString(sql + "\n")

	file.WriteString("\n")
	file.WriteString("-- 检测收款方在途的业务数据\n")
	sql = fmt.Sprintf("SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a join fin_payapply pa on pa.payeeId = a.id WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 2 and pa.extPayStatus not in ('6', 'B', '7', 'C', '9') AND a.arrivalTimeType=0 AND a.branchBankNo IN (%s);",
		inBankNos,
	)
	file.WriteString(sql + "\n")
}
