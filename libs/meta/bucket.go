package meta

import (
	"strconv"
)

type Bucket_item struct {
	Uid            int    `json:"uid"`
	Bname          string `json:"bname"`
	RegionId       int
	Keysecret      string `json:"keysecret"`
	Key            string `json:"key"`
	Imgsource      string `json:"imgsource"`
	Propety        int    `json:"propety"`
	Ctime          string `json:"ctime"`
	Dnsname        string `json:"dnsname"`
	UserDnsName    string `json:"userdnsname"`
	Replication    int    `json:"replication"`
	Styledelimiter string `json:"styledelimiter"`
	Dpstyle        string `json:"dpstyle"`
	Region         string `json:"region"`
	Timeout        int64
}

func Get_hbase_bucketname(uid int, user_bucketname string) string {
	return strconv.Itoa(uid) + "_" + user_bucketname
}
