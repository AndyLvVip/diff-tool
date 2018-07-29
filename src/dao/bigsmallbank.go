package dao

import (
	"model"
	"database/sql"
	"base"
	_ "github.com/go-sql-driver/mysql"
)

func BatchInsert4BigSmallBank(bigSmallBanks []*model.BigSmallBankModel, db *sql.DB) {
	sql, values := buildSqlAndVals4BigSmallBank(bigSmallBanks)

	stmtIns, err := db.Prepare(sql)
	base.CheckErr(err)
	defer stmtIns.Close()

	_, err = stmtIns.Exec(values...)
	base.CheckErr(err)
}

func buildSqlAndVals4BigSmallBank(bigSmallBanks []*model.BigSmallBankModel) (string, []interface{}) {
	sql := "insert into tmp_branchbank (bankNo, bankName, bankCode, areaCode, bankIndex, checkBit) values "
	sqlVar := ""
	var sqlValue []interface{}
	for i := 0; i < len(bigSmallBanks); i++ {
		sqlVar += "(?, ?, ?, ?, ?, ?), "
		sqlValue = append(sqlValue, bigSmallBanks[i].BankNo, bigSmallBanks[i].BankName, bigSmallBanks[i].BankCode, bigSmallBanks[i].AreaCode, bigSmallBanks[i].BankIndex, bigSmallBanks[i].CheckBit)
	}
	sql += sqlVar
	return sql[:len(sql)-2], sqlValue
}

func TruncateBigSmallBank(db *sql.DB) {
	_, err := db.Exec("truncate table tmp_branchbank")
	base.CheckErr(err)
}

func FetchAddedBigSmallBank(db *sql.DB) []*model.BigSmallBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit from tmp_branchbank new left join base_branchbank old on new.bankNo = old.bankNo where old.bankNo is null")
	base.CheckErr(err)
	var bsbSlices []*model.BigSmallBankModel
	for rows.Next() {
		bsb := &model.BigSmallBankModel{}
		err = rows.Scan(&bsb.BankNo, &bsb.BankName, &bsb.BankCode, &bsb.AreaCode, &bsb.BankIndex, &bsb.CheckBit)
		base.CheckErr(err)
		bsbSlices = append(bsbSlices, bsb)
	}
	return bsbSlices
}

func FetchUpdatedBigSmallBank(db *sql.DB) []*model.BigSmallBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName from base_branchbank old join tmp_branchbank new on old.bankNo = new.bankNo where old.bankName <> new.bankName")
	base.CheckErr(err)
	var bsbSlices []*model.BigSmallBankModel
	for rows.Next() {
		bsb := &model.BigSmallBankModel{}
		err = rows.Scan(&bsb.BankNo, &bsb.BankName)
		base.CheckErr(err)
		bsbSlices = append(bsbSlices, bsb)
	}
	return bsbSlices
}

func FetchDeletedBigSmallBank(db *sql.DB) []*model.BigSmallBankModel {
	rows, err := db.Query("select old.bankNo from base_branchbank old left join tmp_branchbank new on old.bankNo = new.bankNo where new.bankNo is null;")
	base.CheckErr(err)
	var bsbSlices []*model.BigSmallBankModel
	for rows.Next() {
		bsb := &model.BigSmallBankModel{}
		err = rows.Scan(&bsb.BankNo)
		base.CheckErr(err)
		bsbSlices = append(bsbSlices, bsb)
	}
	return bsbSlices
}
