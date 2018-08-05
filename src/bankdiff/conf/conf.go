package conf

import (
	"bankdiff/helper"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
	readConfigurationFile()
	Db = initDb()
}

var Db *sql.DB

var Conf = &Configuration{}

func readConfigurationFile() {
	file, err := os.Open("config/config.json")
	helper.CheckErr(err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(Conf)
	helper.CheckErr(err)
	log.Printf("DataSource Configuration: %v\n", Conf.DataSource)
}

type Configuration struct {
	DataSource DataSource
}

type DataSource struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

func (ds DataSource) Name() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", ds.User, ds.Password, ds.Host, ds.Port, ds.Database)
}

func initDb() *sql.DB {
	db, err := sql.Open("mysql", Conf.DataSource.Name())
	helper.CheckErr(err)
	db.SetMaxIdleConns(20)
	return db
}
