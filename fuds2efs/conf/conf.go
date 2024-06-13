package conf

import (
	"io/ioutil"
	"kagamistoreage/fuds2kagamistoreage/libs/toml"
	"os"
	"time"
)

type Config struct {
	Buckets       []string `toml:"buckets"`
	OffsetFile    string   `toml:"offset_file"`
	BasePath      string   `toml:"base_path"`
	LogFilePath   string   `toml:"log_file_path"`
	ParseDuration duration `toml:"parse_duration"`
	UpHost        string
	MgHost        string
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(text))
	return
}

func NewConfig(filepath string) (c *Config, err error) {
	var (
		file *os.File
		blob []byte
	)

	c = new(Config)
	if file, err = os.Open(filepath); err != nil {
		return
	}
	defer file.Close()

	if blob, err = ioutil.ReadAll(file); err != nil {
		return
	}

	err = toml.Unmarshal(blob, c)
	return
}
