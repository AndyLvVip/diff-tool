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
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type SuperBankService struct{}

var service = SuperBankService{}

func NewSuperBankService() *SuperBankService {
	return &service
}

func (service *SuperBankService) Process(now time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	fetcher.Download(now, fetcher.SuperBank)
	file, err := os.Open(fetcher.SuperBank.FilePathAndName(now))
	defer file.Close()
	helper.CheckErr(err)
	reader := bufio.NewReader(transform.NewReader(file, simplifiedchinese.GBK.NewDecoder()))
	service.load(reader)
	service.queryAndGenerate(now)
}

func (*SuperBankService) load(reader *bufio.Reader) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	helper.CheckErr(err)
	defer db.Close()

	_, err = reader.ReadString('\n') //discard the first line
	helper.CheckErr(err)

	dao.NewSuperBank().Truncate(db)

	var sbSlices []*model.SuperBankModel
	last := 0
	cur := 0
	for {
		line, err := reader.ReadString('\n')
		if nil != err {
			if io.EOF != err {
				helper.CheckErr(err)
			} else {
				if len(line) > 0 {
					sbSlices = append(sbSlices, model.ToSuperBank(strings.TrimSpace(line)))
					cur++
				}
				if len(sbSlices) > 0 {
					log.Printf("inserting super banks from %d to %d\n", last, cur)
					dao.NewSuperBank().BatchInsert(sbSlices, db)
					log.Printf("inserted super banks from %d to %d\n", last, cur)
				}
				break
			}
		}

		sbSlices = append(sbSlices, model.ToSuperBank(strings.TrimSpace(line)))
		cur++
		if len(sbSlices) >= 1000 {
			log.Printf("inserting super banks from %d to %d\n", last, cur)
			dao.NewSuperBank().BatchInsert(sbSlices, db)
			log.Printf("inserted super banks from %d to %d\n", last, cur)
			last = cur
			sbSlices = sbSlices[:0]
		}
	}
}

func (service *SuperBankService) queryAndGenerate(now time.Time) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	helper.CheckErr(err)
	defer db.Close()
	added := dao.NewSuperBank().FetchAddedList(db)
	updated := dao.NewSuperBank().FetchUpdatedList(db)
	deleted := dao.NewSuperBank().FetchDeletedList(db)
	service.generateDiffFileSql(now, added, updated, deleted)

	service.PayeeCheckSql(now, updated, deleted)
}

func (*SuperBankService) generateDiffFileSql(now time.Time, added []*model.SuperBankModel, updated []*model.SuperBankModel, deleted []*model.SuperBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/patch/super_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
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
		for _, sb := range added {
			wh.WriteString(sb.AddedSqlScript() + "\n")
		}
	}

	if len(updated) > 0 {
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 变更的记录\n")
		wh.WriteString("-- ----------------------------------------------------------\n")
		for _, sb := range updated {
			wh.WriteString(sb.UpdatedSqlScript() + "\n")
		}
	}

	if len(deleted) > 0 {
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 删除的记录\n")
		wh.WriteString("-- ----------------------------------------------------------\n")
		for _, sb := range deleted {
			wh.WriteString(sb.DeletedSqlScript() + "\n")
		}
	}

	if nil != wh.Err() {
		helper.CheckErr(wh.Err())
	}

	log.Println("generated " + filePathAndName)
}

func (service *SuperBankService) PayeeCheckSql(now time.Time, updated []*model.SuperBankModel, deleted []*model.SuperBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/check/fin_payee_super_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	helper.CheckErr(err)
	file, err := os.Create(filePathAndName)
	helper.CheckErr(err)
	defer file.Close()
	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 实时到账（超级网银联行号））\n")
		file.WriteString("-- 检测是否存在联行号有变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		service.writeCheckSql(file, updated)
	}
	if len(deleted) > 0 {
		file.WriteString("\n\n\n\n\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 实时到账（超级网银联行号）\n")
		file.WriteString("-- 检测是否存在联行号有删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		service.writeCheckSql(file, deleted)
	}
	log.Println("generated " + filePathAndName)
}

func (*SuperBankService) writeCheckSql(file *os.File, vals []*model.SuperBankModel) {
	var args []string
	for _, bsb := range vals {
		args = append(args, helper.SqlValue(bsb.BankNo))
	}
	inBankNos := strings.Join(args, ", ")

	file.WriteString("\n")
	file.WriteString("-- 检测收款方模板数据\n")
	sql := fmt.Sprintf("SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 1 AND a.arrivalTimeType=1 AND a.branchBankNo IN (%s);",
		inBankNos,
	)
	file.WriteString(sql + "\n")

	file.WriteString("\n")
	file.WriteString("-- 检测收款方在途的业务数据\n")
	sql = fmt.Sprintf("SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a join fin_payapply pa on pa.payeeId = a.id WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 2 and pa.extPayStatus not in ('6', 'B', '7', 'C', '9') AND a.arrivalTimeType=1 AND a.branchBankNo IN (%s);",
		inBankNos,
	)
	file.WriteString(sql + "\n")
}
