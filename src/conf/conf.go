package conf

import (
	"log"
	"os"
	"base"
	"encoding/json"
	"fmt"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime|log.Lmicroseconds|log.Llongfile)
	readConfigurationFile()
}

var Conf = &Configuration{}

func readConfigurationFile() {
	file, err := os.Open("config/config.json")
	base.CheckErr(err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(Conf)
	base.CheckErr(err)
	log.Printf("DataSource Configuration: %v\n", Conf.DataSource)
}

type Configuration struct {
	DataSource DataSource
}

type DataSource struct {
	Host string `json:"host"`
	Port string `json:"port"`
	User string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

func (ds DataSource) Name() string{
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", ds.User, ds.Password, ds.Host, ds.Port, ds.Database)
}
