package conf

import (
	"io/ioutil"
	"os"
	//	"path"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	PprofEnable bool
	PprofListen string

	// api
	HttpAddr string
	// proxy
	ProxyAddr string
	//bucket
	BucketAddr string
	//directory
	EfsAddr string
	//mkfile callbak
	CallbakAddr string
	//pfop
	PfopAddr string
	// download domain
	Domain string
	//ak,sk
	BucketAk string
	BucketSk string
	// upload maxfile size
	MaxFileSize int
	//mkfile max blocks more this values callbak mkfile result
	Maxctxs int
	//slice size
	SliceFileSize int64
	//token deadline timeout
	Deadline      int64
	BcacheTimeout int64

	//multipart upload
	PartTmppath          string
	MultipartDataTimeout int64
	DataCleanTimeout     duration
	Hostname             string

	//data process
	DpProxy string
}

// Code to implement the TextUnmarshaler interface for `duration`:
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
	c.Domain = strings.TrimRight(c.Domain, "/")

	return
}
