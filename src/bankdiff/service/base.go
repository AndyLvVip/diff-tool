package service

import (
	"time"
	"sync"
	"os"
	"bankdiff/helper"
	"bufio"
	"golang.org/x/text/transform"
	"golang.org/x/text/encoding/simplifiedchinese"
	"database/sql"
	"bankdiff/conf"
	"bankdiff/model"
	"io"
	"strings"
	"log"
	"math"
	"fmt"
	"path"
)

type IBankService interface {
	Download(time time.Time)
	FilePathAndName(time time.Time) string
	Truncate(db *sql.DB)
	ToModel(string) model.IBankModel
	BatchInsert(*sync.WaitGroup, []model.IBankModel, *sql.DB)
	FetchAddedList(*sql.DB) []model.IBankModel
	FetchUpdatedList(*sql.DB) []model.IBankModel
	FetchDeletedList(*sql.DB) []model.IBankModel
	PatchScriptFilePathAndName(time time.Time) string
	CheckScriptFilePathAndName(time time.Time) string
	CanBeWithdrawalsBank() bool
	CheckWithdrawalsSqlTemplate() string
	CheckPayeeTemplateSqlTemplate() string
	CheckPayeeInProgressBizSqlTemplate() string
}

type BaseBankService struct {

}

var bbs = BaseBankService{}

func NewBaseBankService() *BaseBankService {
	return &bbs
}

func (service *BaseBankService) Process(now time.Time, wg *sync.WaitGroup, ibs IBankService) {
	defer wg.Done()
	ibs.Download(now)

	file, err := os.Open(ibs.FilePathAndName(now))
	helper.CheckErr(err)
	defer file.Close()
	reader := bufio.NewReader(transform.NewReader(file, simplifiedchinese.GBK.NewDecoder()))
	service.dump2Db(reader, ibs)

	service.generatePatchAndCheckScript(now, ibs)
}


func (*BaseBankService) dump2Db(reader *bufio.Reader, ibs IBankService) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	helper.CheckErr(err)
	defer db.Close()

	_, err = reader.ReadString('\n') //discard the first line
	helper.CheckErr(err)

	ibs.Truncate(db)

	var models []model.IBankModel
	now := time.Now()
	for {
		line, err := reader.ReadString('\n')
		if nil != err {
			if io.EOF != err {
				helper.CheckErr(err)
			} else {
				if len(line) > 0 {
					models = append(models, ibs.ToModel(strings.TrimSpace(line)))
				}
				break
			}
		}
		models = append(models, ibs.ToModel(strings.TrimSpace(line)))
	}
	log.Printf("loaded file time: %f\n", time.Now().Sub(now).Seconds())
	count := int(math.Ceil(float64(1.0*len(models)) / 10000))
	wg := &sync.WaitGroup{}
	wg.Add(count)
	now = time.Now()
	for i := 0; i < count; i++ {
		if (i+1)*10000 < len(models) {
			go ibs.BatchInsert(wg, models[i*10000:(i+1)*10000], db)
		} else {
			go ibs.BatchInsert(wg, models[i*10000:], db)
		}
	}
	wg.Wait()
	log.Printf("inserted time: %f\n", time.Now().Sub(now).Seconds())
}

func (service *BaseBankService) generatePatchAndCheckScript(now time.Time, ibs IBankService) {
	db, err := sql.Open("mysql", conf.Conf.DataSource.Name())
	helper.CheckErr(err)
	defer db.Close()
	added := ibs.FetchAddedList(db)
	updated := ibs.FetchUpdatedList(db)
	deleted := ibs.FetchDeletedList(db)

	service.generatePatchScript(now, added, updated, deleted, ibs)

	service.generateCheckScript(now, updated, deleted, ibs)
}

func (*BaseBankService) generatePatchScript(now time.Time, added []model.IBankModel, updated []model.IBankModel, deleted []model.IBankModel, ibs IBankService) {
	err := os.MkdirAll(path.Dir(ibs.PatchScriptFilePathAndName(now)), os.ModePerm)
	helper.CheckErr(err)
	file, err := os.Create(ibs.PatchScriptFilePathAndName(now))
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
	log.Println("generated " + ibs.PatchScriptFilePathAndName(now))
}

func (service *BaseBankService) generateCheckScript(now time.Time, updated []model.IBankModel, deleted []model.IBankModel, ibs IBankService) {
	err := os.MkdirAll(path.Dir(ibs.CheckScriptFilePathAndName(now)), os.ModePerm)
	helper.CheckErr(err)
	file, err := os.Create(ibs.CheckScriptFilePathAndName(now))
	helper.CheckErr(err)
	defer file.Close()
	wh := &helper.WriteHelper{W:file}
	if len(updated) > 0 {
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 普通到账（大小额支行联行号）\n")
		wh.WriteString("-- 检测是否存在联行号有变更的记录\n")
		wh.WriteString("-- ----------------------------------------------------------")

		service.writeCheckSql(wh, updated, ibs)
	}
	if len(deleted) > 0 {
		wh.WriteString("\n\n\n\n\n")
		wh.WriteString("-- ----------------------------------------------------------\n")
		wh.WriteString("-- 普通到账（大小额支行联行号）\n")
		wh.WriteString("-- 检测是否存在联行号有删除的记录\n")
		wh.WriteString("-- ----------------------------------------------------------")

		service.writeCheckSql(wh, deleted, ibs)
	}
	if nil != wh.Err() {
		helper.CheckErr(wh.Err())
	}
	log.Println("generated " + ibs.CheckScriptFilePathAndName(now))
}

func (service *BaseBankService) writeCheckSql(wh *helper.WriteHelper, vals []model.IBankModel, ibs IBankService) {
	var args []string
	for _, bsb := range vals {
		args = append(args, helper.SqlValue(bsb.GetBankNo()))
	}
	inBankNos := strings.Join(args, ", ")
	if ibs.CanBeWithdrawalsBank() {
		wh.WriteString("\n")
		wh.WriteString("-- 检测广发提现账号\n")
		sql := fmt.Sprintf(ibs.CheckWithdrawalsSqlTemplate(),
			inBankNos,
		)
		wh.WriteString(sql + "\n")
	}

	wh.WriteString("\n")
	wh.WriteString("-- 检测收款方模板数据\n")
	sql := fmt.Sprintf(ibs.CheckPayeeTemplateSqlTemplate(),
		inBankNos,
	)
	wh.WriteString(sql + "\n")

	wh.WriteString("\n")
	wh.WriteString("-- 检测收款方在途的业务数据\n")
	sql = fmt.Sprintf(ibs.CheckPayeeInProgressBizSqlTemplate(),
		inBankNos,
	)
	wh.WriteString(sql + "\n")
	if nil != wh.Err() {
		helper.CheckErr(wh.Err())
	}
}
