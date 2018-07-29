package conf

import (
	"log"
	"os"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime|log.Lmicroseconds|log.Llongfile)
}
