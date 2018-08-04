package dao

import (
	"bankdiff/helper"
	"bankdiff/model"
	"database/sql"
	"log"
	"sync"
	"time"
)

type BigSmallBankDao struct {
}

var bsb = BigSmallBankDao{}

func NewBigSmallBankDao() *BigSmallBankDao {
	return &bsb
}

func (dao *BigSmallBankDao) BatchInsert(wg *sync.WaitGroup, bigSmallBanks []*model.BigSmallBankModel, db *sql.DB) {
	defer wg.Done()
	sql, values := dao.buildSqlAndVals(bigSmallBanks)

	stmtIns, err := db.Prepare(sql)
	helper.CheckErr(err)
	defer stmtIns.Close()
	now := time.Now()
	_, err = stmtIns.Exec(values...)
	helper.CheckErr(err)
	log.Printf("exec time: %f\n", time.Now().Sub(now).Seconds())
}

func (*BigSmallBankDao) buildSqlAndVals(bigSmallBanks []*model.BigSmallBankModel) (string, []interface{}) {
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

func (*BigSmallBankDao) Truncate(db *sql.DB) {
	_, err := db.Exec("truncate table tmp_branchbank")
	helper.CheckErr(err)
}

func (*BigSmallBankDao) FetchAddedList(db *sql.DB) []*model.BigSmallBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName, new.bankCode, new.areaCode, new.bankIndex, new.checkBit from tmp_branchbank new left join base_branchbank old on new.bankNo = old.bankNo where old.bankNo is null")
	helper.CheckErr(err)
	var bsbSlices []*model.BigSmallBankModel
	for rows.Next() {
		bsb := &model.BigSmallBankModel{}
		err = rows.Scan(&bsb.BankNo, &bsb.BankName, &bsb.BankCode, &bsb.AreaCode, &bsb.BankIndex, &bsb.CheckBit)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, bsb)
	}
	return bsbSlices
}

func (*BigSmallBankDao) FetchUpdatedList(db *sql.DB) []*model.BigSmallBankModel {
	rows, err := db.Query("select new.bankNo, new.bankName from base_branchbank old join tmp_branchbank new on old.bankNo = new.bankNo where old.bankName <> new.bankName")
	helper.CheckErr(err)
	var bsbSlices []*model.BigSmallBankModel
	for rows.Next() {
		bsb := &model.BigSmallBankModel{}
		err = rows.Scan(&bsb.BankNo, &bsb.BankName)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, bsb)
	}
	return bsbSlices
}

func (*BigSmallBankDao) FetchDeletedList(db *sql.DB) []*model.BigSmallBankModel {
	rows, err := db.Query("select old.bankNo from base_branchbank old left join tmp_branchbank new on old.bankNo = new.bankNo where new.bankNo is null;")
	helper.CheckErr(err)
	var bsbSlices []*model.BigSmallBankModel
	for rows.Next() {
		bsb := &model.BigSmallBankModel{}
		err = rows.Scan(&bsb.BankNo)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, bsb)
	}
	return bsbSlices
}
