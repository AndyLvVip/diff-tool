package model

import (
	"bankdiff/helper"
	"database/sql"
	"fmt"
)

type BigSmallBankModel struct {
	Id        int64
	BankNo    sql.NullString
	BankName  sql.NullString
	BankCode  sql.NullString
	AreaCode  sql.NullString
	BankIndex sql.NullString
	CheckBit  sql.NullString
}

func (bsb *BigSmallBankModel) AddedSqlScript() string {
	return fmt.Sprintf("insert into base_branchbank (bankNo, bankName, bankCode, areaCode, bankIndex, checkBit) values (%s, %s, %s, %s, %s, %s);",
		helper.SqlValue(bsb.BankNo),
		helper.SqlValue(bsb.BankName),
		helper.SqlValue(bsb.BankCode),
		helper.SqlValue(bsb.AreaCode),
		helper.SqlValue(bsb.BankIndex),
		helper.SqlValue(bsb.CheckBit),
	)
}

func (bsb *BigSmallBankModel) UpdatedSqlScript() string {
	return fmt.Sprintf("update base_branchbank set bankName = %s where bankNo = %s;",
		helper.SqlValue(bsb.BankName),
		helper.SqlValue(bsb.BankNo),
	)
}

func (bsb *BigSmallBankModel) DeletedSqlScript() string {
	return fmt.Sprintf("delete from base_branchbank where bankNo = %s;",
		helper.SqlValue(bsb.BankNo),
	)
}

func (bsb *BigSmallBankModel) GetBankNo() sql.NullString {
	return bsb.BankNo
}

func (bsb *BigSmallBankModel) InsertSqlValues() []interface{} {
	var result []interface{}
	result = append(result, bsb.BankNo, bsb.BankName, bsb.BankCode, bsb.AreaCode, bsb.BankIndex, bsb.CheckBit)
	return result
}
