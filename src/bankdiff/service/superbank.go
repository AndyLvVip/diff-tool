package service

import (
	"bankdiff/dao"
	"bankdiff/fetcher"
	"bankdiff/helper"
	"bankdiff/model"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type SuperBankService struct{}

var service = SuperBankService{}

func NewSuperBankService() *SuperBankService {
	return &service
}

func (*SuperBankService) Download(now time.Time) {
	fetcher.Download(now, fetcher.SuperBank)
}

func (*SuperBankService) FilePathAndName(now time.Time) string {
	return fetcher.SuperBank.FilePathAndName(now)
}

func (*SuperBankService) Truncate(db *sql.DB) {
	dao.NewBaseBankDao().Truncate(db, dao.NewSuperBank())
}

func (*SuperBankService) ToModel(line string) model.IBankModel {
	return model.ToSuperBank(line)
}

func (*SuperBankService) BatchInsert(wg *sync.WaitGroup, bsbSlices []model.IBankModel, db *sql.DB) {
	dao.NewBaseBankDao().BatchInsert(wg, bsbSlices, db, dao.NewSuperBank())
}

func (*SuperBankService) FetchAddedList(db *sql.DB) []model.IBankModel {
	return dao.NewBaseBankDao().FetchAddedList(db, dao.NewSuperBank())
}

func (*SuperBankService) FetchUpdatedList(db *sql.DB) []model.IBankModel {
	return dao.NewBaseBankDao().FetchUpdatedList(db, dao.NewSuperBank())
}

func (*SuperBankService) FetchDeletedList(db *sql.DB) []model.IBankModel {
	return dao.NewBaseBankDao().FetchDeletedList(db, dao.NewSuperBank())
}

func (*SuperBankService) PatchScriptFilePathAndName(now time.Time) string {
	return fmt.Sprintf("result/%s/patch/super_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
}

func (*SuperBankService) CheckScriptFilePathAndName(now time.Time) string {
	return fmt.Sprintf("result/%s/check/fin_payee_super_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
}

func (*SuperBankService) CanBeWithdrawalsBank() bool {
	return false
}

func (*SuperBankService) CheckWithdrawalsSqlTemplate() string {
	return ""
}

func (*SuperBankService) CheckPayeeTemplateSqlTemplate() string {
	return "SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 1 AND a.arrivalTimeType=1 AND a.branchBankNo IN (%s);"
}

func (*SuperBankService) CheckPayeeInProgressBizSqlTemplate() string {
	return "SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a join fin_payapply pa on pa.payeeId = a.id WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 2 and pa.extPayStatus not in ('6', 'B', '7', 'C', '9') AND a.arrivalTimeType=1 AND a.branchBankNo IN (%s);"
}