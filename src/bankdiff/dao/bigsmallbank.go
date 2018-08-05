package dao

import (
	"bankdiff/model"
	"database/sql"
)

type BigSmallBankDao struct {
}

var bsb = BigSmallBankDao{}

func NewBigSmallBankDao() *BigSmallBankDao {
	return &bsb
}

func (*BigSmallBankDao) InsertStatement() string {
	return "insert into tmp_branchbank (bankNo, bankName, bankCode, areaCode, bankIndex, checkBit)"
}

func (*BigSmallBankDao) InsertPlaceHolder() string {
	return "(?, ?, ?, ?, ?, ?)"
}

func (*BigSmallBankDao) FetchAddedListQueryStatement() string {
	return "select new.id, new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit from tmp_branchbank new left join base_branchbank old on new.bankNo = old.bankNo where old.bankNo is null"
}

func (*BigSmallBankDao) FetchUpdatedListQueryStatement() string {
	return "select new.id, new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit from base_branchbank old join tmp_branchbank new on old.bankNo = new.bankNo where old.bankName <> new.bankName"
}

func (*BigSmallBankDao) FetchDeletedListQueryStatement() string {
	return "select old.id, old.bankNo, old.bankName, old.bankCode, old.areaCode, old.bankIndex, old.checkBit from base_branchbank old left join tmp_branchbank new on old.bankNo = new.bankNo where new.bankNo is null"
}

func (*BigSmallBankDao) TruncateStatement() string {
	return "truncate table tmp_branchbank"
}

func (*BigSmallBankDao) ScanFrom(rows *sql.Rows) (model.IBankModel, error) {
	bsb := &model.BigSmallBankModel{}
	err := rows.Scan(&bsb.Id, &bsb.BankNo, &bsb.BankName, &bsb.BankCode, &bsb.AreaCode, &bsb.BankIndex, &bsb.CheckBit)
	return bsb, err
}
