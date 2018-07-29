package model

import (
	"database/sql"
	"fmt"
	"base"
	"time"
	"os"
	"path"
	"strings"
	"log"
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
		base.SqlValue(bsb.BankNo),
		base.SqlValue(bsb.BankName),
		base.SqlValue(bsb.BankCode),
		base.SqlValue(bsb.AreaCode),
		base.SqlValue(bsb.BankIndex),
		base.SqlValue(bsb.CheckBit),
	)
}

func (bsb *BigSmallBankModel) UpdatedSqlScript() string {
	return fmt.Sprintf("update base_branchbank set bankName = %s where bankNo = %s;",
		base.SqlValue(bsb.BankName),
		base.SqlValue(bsb.BankNo),
	)
}

func (bsb *BigSmallBankModel) DeletedSqlScript() string {
	return fmt.Sprintf("delete from base_branchbank where bankNo = %s;",
		base.SqlValue(bsb.BankNo),
	)
}




func PayeeCheckSql4BigSmallBank(now time.Time, updated []*BigSmallBankModel, deleted []*BigSmallBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/check/fin_payee_big_small_%s.sql", base.Format2yyyy_MM_dd(now), base.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	base.CheckErr(err)
	file, err := os.Create(filePathAndName)
	base.CheckErr(err)
	defer file.Close()
	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 普通到账（大小额支行联行号）\n")
		file.WriteString("-- 检测是否存在联行号有变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		writeCheckSql4BigSmallBank(file, updated)
	}
	if len(deleted) > 0 {
		file.WriteString("\n\n\n\n\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 普通到账（大小额支行联行号）\n")
		file.WriteString("-- 检测是否存在联行号有删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------")

		writeCheckSql4BigSmallBank(file, deleted)
	}
	log.Println("generated " + filePathAndName)
}


func writeCheckSql4BigSmallBank(file *os.File, vals []*BigSmallBankModel) {
	file.WriteString("\n")
	file.WriteString("-- 检测广发提现账号\n")
	var args []string
	for _, bsb := range vals {
		args = append(args, base.SqlValue(bsb.BankNo))
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
