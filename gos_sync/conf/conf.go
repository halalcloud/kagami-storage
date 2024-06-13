package conf

import (
	"io/ioutil"
	"kagamistoreage/gos_sync/libs/toml"
	"os"
)

type Config struct {
	MURL        string
	DB          string
	TCollection string `toml:"Task_Collection"`
	FCollection string `toml:"Finish_Collection"`

	PrxMSrvAddr string `toml:"ProxyMasterServerAddr"`
	BktMSrvAddr string `toml:"BucketMasterServerAddr"`
	BktMSrvAk   string `toml:"BucketMasterServerAk"`
	BktMSrvSk   string `toml:"BucketMasterServerSk"`
	BktSSrvAddr string `toml:"BucketSlaveServerAddr"`
	BktSSrvAk   string `toml:"BucketSlaveServerAk"`
	BktSSrvSk   string `toml:"BucketSlaveServerSk"`

	Workers            int
	SliceSize          int64
	TaskRemainDuration int64
	UpHost             string
	MgHost             string
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

	if blob, err = ioutil.ReadAll(file); err != nil {
		return
	}

	err = toml.Unmarshal(blob, c)
	return
}
