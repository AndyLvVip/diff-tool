package dao

import (
	"base"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"model"
)


func BatchInsert4SuperBank(superBanks []*model.SuperBankModel, db *sql.DB) {
	sql, values := buildSqlAndVals4SuperBank(superBanks)

	stmtIns, err := db.Prepare(sql)
	base.CheckErr(err)
	defer stmtIns.Close()

	_, err = stmtIns.Exec(values...)
	base.CheckErr(err)
}

func buildSqlAndVals4SuperBank(superBanks []*model.SuperBankModel) (string, []interface{}) {
	sql := "insert into tmp_supercyberbank (bankNo, bankName, bankCode, areaCode, bankIndex, checkBit, bankNickname) values "
	sqlVar := ""
	var sqlValue []interface{}
	for i := 0; i < len(superBanks); i++ {
		sqlVar += "(?, ?, ?, ?, ?, ?, ?), "
		sqlValue = append(sqlValue, superBanks[i].BankNo, superBanks[i].BankName, superBanks[i].BankCode, superBanks[i].AreaCode, superBanks[i].BankIndex, superBanks[i].CheckBit, superBanks[i].BankNickname)
	}
	sql += sqlVar
	return sql[:len(sql)-2], sqlValue
}



func TruncateSuperBank(db *sql.DB) {
	_, err := db.Exec("truncate table tmp_supercyberbank")
	base.CheckErr(err)
}

func FetchAddedSuperBank(db *sql.DB) []*model.SuperBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit, new.bankNickname from tmp_supercyberbank new left join base_supercyberbank old on new.bankNo = old.bankNo where old.bankNo is null")
	base.CheckErr(err)
	var bsbSlices []*model.SuperBankModel
	for rows.Next() {
		sb := &model.SuperBankModel{}
		err = rows.Scan(&sb.BankNo, &sb.BankName, &sb.BankCode, &sb.AreaCode, &sb.BankIndex, &sb.CheckBit, &sb.BankNickname)
		base.CheckErr(err)
		bsbSlices = append(bsbSlices, sb)
	}
	return bsbSlices
}

func FetchUpdatedSuperBank(db *sql.DB) []*model.SuperBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName from base_supercyberbank old join tmp_supercyberbank new on old.bankNo = new.bankNo where old.bankName <> new.bankName")
	base.CheckErr(err)
	var bsbSlices []*model.SuperBankModel
	for rows.Next() {
		sb := &model.SuperBankModel{}
		err = rows.Scan(&sb.BankNo, &sb.BankName)
		base.CheckErr(err)
		bsbSlices = append(bsbSlices, sb)
	}
	return bsbSlices
}

func FetchDeletedSuperBank(db *sql.DB) []*model.SuperBankModel {
	rows, err := db.Query("select old.bankNo from base_supercyberbank old left join tmp_supercyberbank new on old.bankNo = new.bankNo where new.bankNo is null;")
	base.CheckErr(err)
	var bsbSlices []*model.SuperBankModel
	for rows.Next() {
		sb := &model.SuperBankModel{}
		err = rows.Scan(&sb.BankNo)
		base.CheckErr(err)
		bsbSlices = append(bsbSlices, sb)
	}
	return bsbSlices
}
