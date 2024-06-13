package conf

import (
	"io/ioutil"
	"os"
	//	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	ApiListen string
	EfsAddr   string

	MongoUserName string
	MongoPasswd   string
	MongoAddr     string
}

func NewConfig(conf string) (c *Config, err error) {
	var (
		file *os.File
		blob []byte
	)
	c = new(Config)
	if file, err = os.Open(conf); err != nil {
		return
	}
	if blob, err = ioutil.ReadAll(file); err != nil {
		return
	}
	if err = toml.Unmarshal(blob, c); err != nil {
		return
	}

	return
}
