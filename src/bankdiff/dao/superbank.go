package dao

import (
	"bankdiff/helper"
	"bankdiff/model"
	"database/sql"
)

type SuperBankDao struct {
}

var sb = SuperBankDao{}

func NewSuperBank() *SuperBankDao {
	return &sb
}

func (dao *SuperBankDao) BatchInsert(superBanks []*model.SuperBankModel, db *sql.DB) {
	sql, values := dao.buildSqlAndVals(superBanks)

	stmtIns, err := db.Prepare(sql)
	helper.CheckErr(err)
	defer stmtIns.Close()

	_, err = stmtIns.Exec(values...)
	helper.CheckErr(err)
}

func (*SuperBankDao) buildSqlAndVals(superBanks []*model.SuperBankModel) (string, []interface{}) {
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

func (*SuperBankDao) Truncate(db *sql.DB) {
	_, err := db.Exec("truncate table tmp_supercyberbank")
	helper.CheckErr(err)
}

func (*SuperBankDao) FetchAddedList(db *sql.DB) []*model.SuperBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit, new.bankNickname from tmp_supercyberbank new left join base_supercyberbank old on new.bankNo = old.bankNo where old.bankNo is null")
	helper.CheckErr(err)
	var bsbSlices []*model.SuperBankModel
	for rows.Next() {
		sb := &model.SuperBankModel{}
		err = rows.Scan(&sb.BankNo, &sb.BankName, &sb.BankCode, &sb.AreaCode, &sb.BankIndex, &sb.CheckBit, &sb.BankNickname)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, sb)
	}
	return bsbSlices
}

func (*SuperBankDao) FetchUpdatedList(db *sql.DB) []*model.SuperBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName from base_supercyberbank old join tmp_supercyberbank new on old.bankNo = new.bankNo where old.bankName <> new.bankName")
	helper.CheckErr(err)
	var bsbSlices []*model.SuperBankModel
	for rows.Next() {
		sb := &model.SuperBankModel{}
		err = rows.Scan(&sb.BankNo, &sb.BankName)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, sb)
	}
	return bsbSlices
}

func (*SuperBankDao) FetchDeletedList(db *sql.DB) []*model.SuperBankModel {
	rows, err := db.Query("select old.bankNo from base_supercyberbank old left join tmp_supercyberbank new on old.bankNo = new.bankNo where new.bankNo is null;")
	helper.CheckErr(err)
	var bsbSlices []*model.SuperBankModel
	for rows.Next() {
		sb := &model.SuperBankModel{}
		err = rows.Scan(&sb.BankNo)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, sb)
	}
	return bsbSlices
}
