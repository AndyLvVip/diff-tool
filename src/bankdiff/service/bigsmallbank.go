package service

import (
	"bankdiff/dao"
	"bankdiff/fetcher"
	"bankdiff/helper"
	"bankdiff/model"
	"fmt"
	"sync"
	"time"
)

type BigSmallBankService struct {
}

var bsb = BigSmallBankService{}

func NewBigSmallBankService() *BigSmallBankService {
	return &bsb
}

func (*BigSmallBankService) Download(now time.Time) {
	fetcher.Download(now, fetcher.BigSmallBank)
}

func (*BigSmallBankService) FilePathAndName(now time.Time) string {
	return fetcher.BigSmallBank.FilePathAndName(now)
}

func (*BigSmallBankService) Truncate() {
	dao.NewBaseBankDao().Truncate(dao.NewBigSmallBankDao())
}

func (*BigSmallBankService) ToModel(line string) model.IBankModel {
	return model.ToBigSmallBank(line)
}

func (*BigSmallBankService) BatchInsert(wg *sync.WaitGroup, bsbSlices []model.IBankModel) {
	dao.NewBaseBankDao().BatchInsert(wg, bsbSlices, dao.NewBigSmallBankDao())
}

func (*BigSmallBankService) FetchAddedList() []model.IBankModel {
	return dao.NewBaseBankDao().FetchAddedList(dao.NewBigSmallBankDao())
}

func (*BigSmallBankService) FetchUpdatedList() []model.IBankModel {
	return dao.NewBaseBankDao().FetchUpdatedList(dao.NewBigSmallBankDao())
}

func (*BigSmallBankService) FetchDeletedList() []model.IBankModel {
	return dao.NewBaseBankDao().FetchDeletedList(dao.NewBigSmallBankDao())
}

func (*BigSmallBankService) PatchScriptFilePathAndName(now time.Time) string {
	return fmt.Sprintf("result/%s/patch/big_small_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
}

func (*BigSmallBankService) CheckScriptFilePathAndName(now time.Time) string {
	return fmt.Sprintf("result/%s/check/fin_payee_big_small_%s.sql", helper.Format2yyyy_MM_dd(now), helper.Format2yyyyMMddHHmmss(now))
}

func (*BigSmallBankService) CanBeWithdrawalsBank() bool {
	return true
}

func (*BigSmallBankService) CheckWithdrawalsSqlTemplate() string {
	return "SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 1 AND a.arrivalTimeType=0 AND a.branchBankNo IN (%s);"
}

func (*BigSmallBankService) CheckPayeeTemplateSqlTemplate() string {
	return "SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 1 AND a.arrivalTimeType=0 AND a.branchBankNo IN (%s);"
}

func (*BigSmallBankService) CheckPayeeInProgressBizSqlTemplate() string {
	return "SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a join fin_payapply pa on pa.payeeId = a.id WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 2 and pa.extPayStatus not in ('6', 'B', '7', 'C', '9') AND a.arrivalTimeType=0 AND a.branchBankNo IN (%s);"
}
