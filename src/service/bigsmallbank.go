package service

import (
	"bufio"
	"database/sql"
	"base"
	"dao"
	"model"
	"io"
	"strings"
	"fmt"
	"time"
	"os"
	"path"
	"log"
)

func LoadBigSmallBank(reader *bufio.Reader) {
	db, err := sql.Open("mysql", "andy:password@/ucacc_dev")
	base.CheckErr(err)
	defer db.Close()

	_, err = reader.ReadString('\n') //discard the first line
	base.CheckErr(err)

	dao.TruncateBigSmallBank(db)

	var bsbSlices []*model.BigSmallBankModel
	last := 0
	cur := 0
	for {
		line, err := reader.ReadString('\n')
		if nil != err {
			if io.EOF != err {
				base.CheckErr(err)
			} else {
				if len(line) > 0 {
					bsbSlices = append(bsbSlices, model.ToBigSmallBank(strings.TrimSpace(line)))
					cur++
				}
				if len(bsbSlices) > 0 {
					log.Printf("inserting big small banks from %d to %d\n", last, cur)
					dao.BatchInsert4BigSmallBank(bsbSlices, db)
				}
				break
			}
		}

		bsbSlices = append(bsbSlices, model.ToBigSmallBank(strings.TrimSpace(line)))
		cur++
		if len(bsbSlices) == 10000 {
			log.Printf("inserting big small banks from %d to %d\n", last, cur)
			dao.BatchInsert4BigSmallBank(bsbSlices, db)
			last = cur
			bsbSlices = bsbSlices[:0]
		}
	}
}



func QueryAndGenerate4BigSmallBank(now time.Time) {
	db, err := sql.Open("mysql", "andy:password@/ucacc_dev")
	base.CheckErr(err)
	defer db.Close()
	added := dao.FetchAddedBigSmallBank(db)
	updated := dao.FetchUpdatedBigSmallBank(db)
	deleted := dao.FetchDeletedBigSmallBank(db)

	GenerateDiffFileSql4BigSmallBank(now, added, updated, deleted)

	model.PayeeCheckSql4BigSmallBank(now, updated, deleted)
}


func GenerateDiffFileSql4BigSmallBank(now time.Time, added []*model.BigSmallBankModel, updated []*model.BigSmallBankModel, deleted []*model.BigSmallBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/patch/big_small_%s.sql", base.Format2yyyy_MM_dd(now), base.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	base.CheckErr(err)
	file, err := os.Create(filePathAndName)
	base.CheckErr(err)
	defer file.Close()

	if len(added) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 新增的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range added {

			file.WriteString(bsd.AddedSqlScript() + "\n")
		}
	}

	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range updated {
			file.WriteString(bsd.UpdatedSqlScript() + "\n")
		}
	}

	if len(deleted) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, bsd := range deleted {
			file.WriteString(bsd.DeletedSqlScript() + "\n")
		}
	}
	log.Println("generated " + filePathAndName)
}
