package conf

import (
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
)

type Config struct {
	PprofEnable bool
	PprofListen string

	// api
	HttpAddr string

	DatabaseAddr string

	DatabaseName string

	DatabaseUname string

	Databasepasswd string

	DatabasePort string
	Ak           string
	Sk           string

	Timeout int

	Directoryhttpaddr string
	Dnssuffix         string
}

// NewConfig new a config.
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
