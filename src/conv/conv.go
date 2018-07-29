package conv

import (
	"model"
	"regexp"
	"base"
	"database/sql"
	"strings"
)

func ToBigSmallBank(line string) *model.BigSmallBankModel{
	bankNo := GetBankNo(line)
	return &model.BigSmallBankModel{
		BankNo:ToNullString(bankNo),
		BankIndex:ToNullString(GetBankIndex(bankNo)),
		BankCode:ToNullString(GetBankCode(bankNo)),
		BankName:ToNullString(GetBankName(line, bankNo)),
		AreaCode:ToNullString(GetAreaCode(bankNo)),
		CheckBit:ToNullString(GetCheckBit(bankNo)),
	}
}

func ToSuperBank(line string) *model.SuperBankModel {
	bankNo := GetBankNo(line)
	bankName := GetBankName(line, bankNo)
	return &model.SuperBankModel{
		BankNo:ToNullString(bankNo),
		BankName:ToNullString(bankName),
		BankCode:ToNullString(GetBankCode(bankNo)),
		BankIndex:ToNullString(GetBankIndex(bankNo)),
		BankNickname:ToNullString(bankName),
		AreaCode:ToNullString(GetAreaCode(bankNo)),
		CheckBit:ToNullString(GetCheckBit(bankNo)),
	}
}

func GetBankNo(line string) string {
	reg, err := regexp.Compile("^\\d{1,12}")
	base.CheckErr(err)
	return reg.FindString(line)
}

func GetBankName(line string, bankNo string) string {
	return strings.Replace(line, bankNo, "", 1)
}

func GetBankCode(bankNo string) string {
	return extractContent(bankNo, 0, 3)
}

func extractContent(bankNo string, beginIndex int, endIndex int) string {
	if beginIndex >= len(bankNo) {
		return ""
	}
	if endIndex < len(bankNo) {
		return bankNo[beginIndex:endIndex]
	}else {
		return bankNo[beginIndex:]
	}
}

func GetAreaCode(bankNo string) string {
	return extractContent(bankNo, 3, 7)
}

func GetBankIndex(bankNo string) string {
	return extractContent(bankNo, 7, 11)
}

func GetCheckBit(bankNo string) string {
	return extractContent(bankNo, 11, len(bankNo))
}

func ToNullString(str string) sql.NullString {
	if 0 == len(str) {
		return sql.NullString{String:"", Valid:false}
	}
	return sql.NullString{String: str, Valid: true}
}