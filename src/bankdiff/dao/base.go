package dao

import (
	"sync"
	"bankdiff/model"
	"database/sql"
	"bankdiff/helper"
	"time"
	"log"
)

type IBankDao interface {
	InsertStatement() string
	TruncateStatement() string
	InsertPlaceHolder() string
	FetchAddedListQueryStatement() string
	FetchUpdatedListQueryStatement() string
	FetchDeletedListQueryStatement() string
	ScanFrom(rows *sql.Rows) (model.IBankModel, error)

}

type BaseBankDao struct {

}

var dao = BaseBankDao{}

func NewBaseBankDao() *BaseBankDao {
	return &dao
}

func (*BaseBankDao) BatchInsert(wg *sync.WaitGroup, bsbSlices []model.IBankModel, db *sql.DB, ibd IBankDao) {
	defer wg.Done()
	sql, values := dao.buildSqlAndVals(bsbSlices, ibd)

	stmtIns, err := db.Prepare(sql)
	helper.CheckErr(err)
	defer stmtIns.Close()
	now := time.Now()
	_, err = stmtIns.Exec(values...)
	helper.CheckErr(err)
	log.Printf("exec time: %f\n", time.Now().Sub(now).Seconds())
}


func (*BaseBankDao) buildSqlAndVals(bigSmallBanks []model.IBankModel, ibd IBankDao) (string, []interface{}) {
	sql := ibd.InsertStatement() + " values "
	sqlVar := ""
	var sqlValue []interface{}
	for i := 0; i < len(bigSmallBanks); i++ {
		sqlVar += ibd.InsertPlaceHolder()
		sqlValue = append(sqlValue, bigSmallBanks[i].InsertSqlValues()...)
	}
	sql += sqlVar
	return sql[:len(sql)-2], sqlValue
}


func (*BaseBankDao) FetchAddedList(db *sql.DB, ibd IBankDao) []model.IBankModel {
	rows, err := db.Query(ibd.FetchAddedListQueryStatement())
	helper.CheckErr(err)
	var bsbSlices []model.IBankModel
	for rows.Next() {
		m, err := ibd.ScanFrom(rows)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, m)
	}
	return bsbSlices
}

func (*BaseBankDao) FetchUpdatedList(db *sql.DB, ibd IBankDao) []model.IBankModel {
	rows, err := db.Query(ibd.FetchUpdatedListQueryStatement())
	helper.CheckErr(err)
	var bsbSlices []model.IBankModel
	for rows.Next() {
		m, err := ibd.ScanFrom(rows)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, m)
	}
	return bsbSlices
}

func (*BaseBankDao) FetchDeletedList(db *sql.DB, ibd IBankDao) []model.IBankModel {
	rows, err := db.Query(ibd.FetchDeletedListQueryStatement())
	helper.CheckErr(err)
	var bsbSlices []model.IBankModel
	for rows.Next() {
		m, err := ibd.ScanFrom(rows)
		helper.CheckErr(err)
		bsbSlices = append(bsbSlices, m)
	}
	return bsbSlices
}

func (*BaseBankDao) Truncate(db *sql.DB, ibd IBankDao) {
	_, err := db.Exec(ibd.TruncateStatement())
	helper.CheckErr(err)
}