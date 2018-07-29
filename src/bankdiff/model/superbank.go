package model

import (
	"bankdiff/base"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

func PayeeCheckSql4SuperBank(now time.Time, updated []*SuperBankModel, deleted []*SuperBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/check/fin_payee_super_%s.sql", base.Format2yyyy_MM_dd(now), base.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	base.CheckErr(err)
	file, err := os.Create(filePathAndName)
	base.CheckErr(err)
	defer file.Close()
	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 实时到账（超级网银联行号））\n")
		file.WriteString("-- 检测是否存在联行号有变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		writeCheckSql4SuperBank(file, updated)
	}
	if len(deleted) > 0 {
		file.WriteString("\n\n\n\n\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 实时到账（超级网银联行号）\n")
		file.WriteString("-- 检测是否存在联行号有删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		writeCheckSql4SuperBank(file, deleted)
	}
	log.Println("generated " + filePathAndName)
}

func writeCheckSql4SuperBank(file *os.File, vals []*SuperBankModel) {
	var args []string
	for _, bsb := range vals {
		args = append(args, base.SqlValue(bsb.BankNo))
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
		base.SqlValue(sb.BankNo),
		base.SqlValue(sb.BankName),
		base.SqlValue(sb.BankCode),
		base.SqlValue(sb.AreaCode),
		base.SqlValue(sb.BankIndex),
		base.SqlValue(sb.CheckBit),
		base.SqlValue(sb.BankNickname),
	)
}

func (sb *SuperBankModel) UpdatedSqlScript() string {
	return fmt.Sprintf("update base_supercyberbank set bankName = %s where bankNo = %s;",
		base.SqlValue(sb.BankName),
		base.SqlValue(sb.BankNo),
	)
}

func (sb *SuperBankModel) DeletedSqlScript() string {
	return fmt.Sprintf("delete from base_supercyberbank where bankNo = %s;",
		base.SqlValue(sb.BankNo),
	)
}
