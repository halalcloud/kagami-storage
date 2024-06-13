package conf

import (
	"efs/store/needle"
	"io/ioutil"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Pprof           bool
	PprofListen     string
	StatListen      string
	ApiListen       string
	AdminListen     string
	RebalanceListen string

	NeedleMaxSize             int
	BlockMaxSize              int
	BatchMaxNum               int
	ReservedFreevolumePerDisk int //每个磁盘预留多少个freevolume
	CompactNums               int //超过多少个delete个数后触发卷做卷压缩
	DfshellPath               string

	Store     *Store
	Volume    *Volume
	Block     *Block
	Index     *Index
	Limit     *Limit
	Zookeeper *Zookeeper
}

type Store struct {
	VolumeIndex        string
	FreeVolumeIndex    string
	RebalanceIndex     string
	RebalanceDestIndex string
	RecoveryIndex      string
}

type Volume struct {
	SyncDelete      int
	SyncDeleteDelay Duration
}

type Block struct {
	BufferSize    int `toml:"-"`
	SyncWrite     int
	Syncfilerange bool
}

type Index struct {
	BufferSize    int
	MergeDelay    Duration
	MergeWrite    int
	RingBuffer    int
	SyncWrite     int
	Syncfilerange bool
}

type Zookeeper struct {
	Root      string
	Rack      string
	ServerId  string
	Volume    string
	Rebalance string
	Recovery  string
	Addrs     []string
	Timeout   Duration
}

type Rate struct {
	Rate  float64
	Brust int
}

type Limit struct {
	Read   *Rate
	Write  *Rate
	Delete *Rate
}

// Code to implement the TextUnmarshaler interface for `Duration`:
//
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
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
	if err = toml.Unmarshal(blob, c); err == nil {
		c.BlockMaxSize = needle.Size(c.NeedleMaxSize)
		c.Block.BufferSize = needle.Size(c.NeedleMaxSize)
	}
	return
}
