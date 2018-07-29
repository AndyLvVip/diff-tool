package base

import (
	"database/sql"
	"fmt"
	"time"
)

func CheckErr(err error) {
	if nil != err {
		panic(err)
	}
}

func IfThenElse(cond bool, th interface{}, el interface{}) interface{} {
	if cond {
		return th
	}
	return el
}

func SqlValue(nullString sql.NullString) string {
	return fmt.Sprintf("%s", IfThenElse(nullString.Valid, fmt.Sprintf("'%s'", nullString.String), "NULL"))
}

func Format2yyyyMMddHHmmss(datetime time.Time) string {
	return datetime.Format("20060102150405")
}

func Format2yyyy_MM_dd(now time.Time) string {
	return now.Format("2006-01-02")
}
