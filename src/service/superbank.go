package service

import (
	"base"
	"bufio"
	"conv"
	"dao"
	"database/sql"
	"fmt"
	"io"
	"model"
	"os"
	"path"
	"strings"
	"time"
)

func LoadSuperBank(reader *bufio.Reader) {
	db, err := sql.Open("mysql", "andy:password@/ucacc_dev")
	base.CheckErr(err)
	defer db.Close()

	_, err = reader.ReadString('\n') //discard the first line
	base.CheckErr(err)

	dao.TruncateSuperBank(db)

	var sbSlices []*model.SuperBankModel
	last := 0
	cur := 0
	for {
		line, err := reader.ReadString('\n')
		if nil != err {
			if io.EOF != err {
				base.CheckErr(err)
			} else {
				if len(line) > 0 {
					sbSlices = append(sbSlices, model.ToSuperBank(strings.TrimSpace(line)))
					cur++
				}
				if len(sbSlices) > 0 {
					fmt.Printf("inserting super banks from %d to %d\n", last, cur)
					dao.BatchInsert4SuperBank(sbSlices, db)
				}
				break
			}
		}

		sbSlices = append(sbSlices, model.ToSuperBank(strings.TrimSpace(line)))
		cur++
		if len(sbSlices) == 10000 {
			fmt.Printf("inserting super banks from %d to %d\n", last, cur)
			dao.BatchInsert4SuperBank(sbSlices, db)
			last = cur
			sbSlices = sbSlices[:0]
		}
	}
}

func QueryAndGenerate4SuperBank(now time.Time) {
	db, err := sql.Open("mysql", "andy:password@/ucacc_dev")
	base.CheckErr(err)
	defer db.Close()
	added := dao.FetchAddedSuperBank(db)
	updated := dao.FetchUpdatedSuperBank(db)
	deleted := dao.FetchDeletedSuperBank(db)
	GenerateDiffFileSql4SuperBank(now, added, updated, deleted)

	model.PayeeCheckSql4SuperBank(now, updated, deleted)
}

func GenerateDiffFileSql4SuperBank(now time.Time, added []*model.SuperBankModel, updated []*model.SuperBankModel, deleted []*model.SuperBankModel) {
	filePathAndName := fmt.Sprintf("result/%s/patch/super_%s.sql", base.Format2yyyy_MM_dd(now), base.Format2yyyyMMddHHmmss(now))
	err := os.MkdirAll(path.Dir(filePathAndName), os.ModePerm)
	base.CheckErr(err)
	file, err := os.Create(filePathAndName)
	base.CheckErr(err)
	defer file.Close()

	if len(added) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 新增的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, sb := range added {

			file.WriteString(sb.AddedSqlScript() + "\n")
		}
	}

	if len(updated) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 变更的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, sb := range updated {
			file.WriteString(sb.UpdatedSqlScript() + "\n")
		}
	}

	if len(deleted) > 0 {
		file.WriteString("-- ----------------------------------------------------------\n")
		file.WriteString("-- 删除的记录\n")
		file.WriteString("-- ----------------------------------------------------------\n")
		for _, sb := range deleted {
			file.WriteString(sb.DeletedSqlScript() + "\n")
		}
	}
	fmt.Println("generated " + filePathAndName)
}
