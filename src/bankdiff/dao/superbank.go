package dao

import (
	"bankdiff/model"
	"database/sql"
)

type SuperBankDao struct {
}

var sb = SuperBankDao{}

func NewSuperBank() *SuperBankDao {
	return &sb
}

func (*SuperBankDao) InsertStatement() string {
	return "insert into tmp_supercyberbank (bankNo, bankName, bankCode, areaCode, bankIndex, checkBit, bankNickname)"
}

func (*SuperBankDao) InsertPlaceHolder() string {
	return "(?, ?, ?, ?, ?, ?, ?), "
}

func (*SuperBankDao) FetchAddedListQueryStatement() string {
	return "select new.id, new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit, new.bankNickname from tmp_supercyberbank new left join base_supercyberbank old on new.bankNo = old.bankNo where old.bankNo is null"
}

func (*SuperBankDao) FetchUpdatedListQueryStatement() string {
	return "select new.id, new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit, new.bankNickname from base_supercyberbank old join tmp_supercyberbank new on old.bankNo = new.bankNo where old.bankName <> new.bankName"
}

func (*SuperBankDao) FetchDeletedListQueryStatement() string {
	return "select old.id, old.bankNo, old.bankName, old.bankCode, old.areaCode, old.bankIndex, old.checkBit, old.bankNickname from base_supercyberbank old left join tmp_supercyberbank new on old.bankNo = new.bankNo where new.bankNo is null"
}

func (*SuperBankDao) TruncateStatement() string {
	return "truncate table tmp_supercyberbank"
}

func (*SuperBankDao) ScanFrom(rows *sql.Rows) (model.IBankModel, error) {
	bsb := &model.SuperBankModel{}
	err := rows.Scan(&bsb.Id, &bsb.BankNo, &bsb.BankName, &bsb.BankCode, &bsb.AreaCode, &bsb.BankIndex, &bsb.CheckBit, &bsb.BankNickname)
	return bsb, err
}
