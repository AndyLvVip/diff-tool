package model

import (
	"bankdiff/helper"
	"database/sql"
	"fmt"
)

type SuperBankModel struct {
	Id           int64
	BankNo       sql.NullString
	BankName     sql.NullString
	BankCode     sql.NullString
	AreaCode     sql.NullString
	BankIndex    sql.NullString
	CheckBit     sql.NullString
	BankNickname sql.NullString
}

func (sb *SuperBankModel) AddedSqlScript() string {
	return fmt.Sprintf("insert into base_supercyberbank (bankNo, bankName, bankCode, areaCode, bankIndex, checkBit, bankNickname) values (%s, %s, %s, %s, %s, %s, %s);",
		helper.SqlValue(sb.BankNo),
		helper.SqlValue(sb.BankName),
		helper.SqlValue(sb.BankCode),
		helper.SqlValue(sb.AreaCode),
		helper.SqlValue(sb.BankIndex),
		helper.SqlValue(sb.CheckBit),
		helper.SqlValue(sb.BankNickname),
	)
}

func (sb *SuperBankModel) UpdatedSqlScript() string {
	return fmt.Sprintf("update base_supercyberbank set bankName = %s where bankNo = %s;",
		helper.SqlValue(sb.BankName),
		helper.SqlValue(sb.BankNo),
	)
}

func (sb *SuperBankModel) DeletedSqlScript() string {
	return fmt.Sprintf("delete from base_supercyberbank where bankNo = %s;",
		helper.SqlValue(sb.BankNo),
	)
}
