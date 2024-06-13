package hbase

import (
	"kagamistoreage/directory/conf"
	"kagamistoreage/directory/hbase/hbasethrift"
	log "kagamistoreage/log/glog"

	"github.com/apache/thrift/lib/go/thrift"
)

var (
	hbasePool *Pool
	config    *conf.Config
)

func Init(config *conf.Config) error {
	config = config
	// init hbase thrift pool
	hbasePool = New(func() (c *hbasethrift.THBaseServiceClient, err error) {
		var trans thrift.TTransport
		if trans, err = thrift.NewTSocketTimeout(config.HBase.Addr, config.HBase.Timeout.Duration); err != nil {
			log.Error("thrift.NewTSocketTimeout error(%v)", err)
			return
		}
		trans = thrift.NewTFramedTransport(trans)
		c = hbasethrift.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if err = trans.Open(); err != nil {
			log.Error("trans.Open error(%v)", err)
		}
		return
	}, func(c *hbasethrift.THBaseServiceClient) error {
		if c != nil && c.Transport != nil {
			c.Transport.Close()
		}
		return nil
	}, config.HBase.MaxIdle)
	hbasePool.MaxActive = config.HBase.MaxActive
	hbasePool.IdleTimeout = config.HBase.LvsTimeout.Duration
	hbasePool.GetConTimeout = config.HBase.GetConTimeout
	ScanHLInit()
	return nil
}

func Close() {
}
