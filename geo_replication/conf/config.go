package conf

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	PprofEnable bool
	PprofListen string

	// api
	DownloadPort string
	BlockSize    string
	UploadMethod string
	UploadMkfile string
	DeleteMethod string
	DeleteBucket string
	Copy         string
	Mv           string

	//bucket
	BcacheTimeout int
	BucketAk      string
	BucketSk      string
	BucketAddr    string

	//dest ftp
	RootPath string

	//kafka
	ZookeeperString string
	ConsumerGroup   string
	Topic           string
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
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
