package main

import (
	"kagamistoreage/geo_replication/bucket"
	"kagamistoreage/geo_replication/conf"
	"kagamistoreage/geo_replication/efs"

	"encoding/base64"
	"kagamistoreage/geo_replication/sync_dest"
	log "kagamistoreage/log/glog"

	//"fmt"

	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

type Rsync_efs struct {
	c          *conf.Config
	rsync_dest *sync_dest.Dest_io
	rsync_efs  *efs.Src_io
	ibucket    *bucket.BucketInfo
}

func Sync_init(c *conf.Config) (sync *Rsync_efs, err error) {
	sync = &Rsync_efs{}
	sync.c = c
	sync.rsync_dest, err = sync_dest.New(c)
	if err != nil {
		log.Errorf("Init sync dest failed %v", err)
		return
	}

	sync.rsync_efs, err = efs.New(c)
	if err != nil {
		log.Errorf("Init sync efs failed %v", err)
		return
	}
	sync.ibucket = bucket.Bucket_init(c)

	return
}

func (sync *Rsync_efs) Sync_start() (err error) {
	var (
		zookeeperNodes           []string
		zkstring, cgroup, topics string
		consumer                 *consumergroup.ConsumerGroup
		fname, fbucket           []byte
		retcode                  int
	)
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	zkstring = sync.c.ZookeeperString
	cgroup = sync.c.ConsumerGroup
	topics = sync.c.Topic

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zkstring)

	kafkaTopics := strings.Split(topics, ",")

	consumer, err = consumergroup.JoinConsumerGroup(cgroup, kafkaTopics, zookeeperNodes, config)
	if err != nil {
		log.Errorf("joinconsumergroup failed %v", err)
		return
	}
	for msg := range consumer.Messages() {
		tmpmsg := string(msg.Value)
		ss := strings.Split(tmpmsg, " ")

		if len(ss) < 5 {
			log.Errorf("this message %s is invalid", tmpmsg)
			for i, ts := range ss {
				log.Errorf("i=%d value= %s", i, ts)
			}
			consumer.CommitUpto(msg)
			continue
		}
		LEN := len(ss)
		LEN = LEN - 1
		ss1 := strings.Split(ss[LEN], "	")
		if len(ss1) != 9 {
			log.Errorf("this message %s is invalid", tmpmsg)
			consumer.CommitUpto(msg)
			continue
		}
		method := ss1[0]
		bucket := ss1[1]
		filename := ss1[2]
		overflag := ss1[3]
		oldfsize := ss1[4]
		filesize := ss1[5]
		result := ss1[7]

		if method == sync.c.UploadMethod || method == sync.c.UploadMkfile || method == sync.c.DeleteMethod || method == sync.c.Copy || method == sync.c.Mv || method == sync.c.DeleteBucket {
			if result != "200" {
				consumer.CommitUpto(msg)
				log.Infof("result not 200 ")
				continue
			}
		} else {
			consumer.CommitUpto(msg)
			log.Infof("log method is %s pass ", method)
			continue
		}
		if bucket == "-" {
			consumer.CommitUpto(msg)
			log.Infof("bucket is - pass ")
			continue
		}
		fname, err = base64.URLEncoding.DecodeString(filename)
		if err != nil {
			log.Errorf("decode filename (%s)failed %v pass message(%s)", filename, err, tmpmsg)
			consumer.CommitUpto(msg)
			continue
		}

		if method == sync.c.Copy || method == sync.c.Mv {
			fbucket, err = base64.URLEncoding.DecodeString(bucket)
			if err != nil {
				log.Errorf("decode bucket (%s)failed %v pass message(%s)", bucket, err, tmpmsg)
				consumer.CommitUpto(msg)
				continue
			}
			bucket = string(fbucket)
		}

		log.Infof("get message %s ", tmpmsg)
		retcode, err = sync.File_sync(method, bucket, string(fname), overflag, oldfsize, filesize)
		if err != nil && retcode == 612 {
			log.Errorf("sync bucket %s filename %s failed file is not exsit", bucket, string(fname))
			consumer.CommitUpto(msg)
			continue
		}

		// if time is not sequential,file not rsync but delet file and delete null bucket
		if err != nil && (method == sync.c.DeleteMethod || method == sync.c.DeleteBucket) {
			log.Errorf("rm -rf bucket=%s filename=%s failed (%v) pass ", bucket, string(fname), err)
			consumer.CommitUpto(msg)
			continue
		}

		if err != nil {
			log.Errorf("sync file failed %v", err)
			break
		}
		consumer.CommitUpto(msg)

	}
	return
}

func (sync *Rsync_efs) File_copy_data(bucket, filename, destname, filesize, dnsname string) (retcode int, err error) {
	var (
		fsize, b, e, blocksize int64
		filefd                 *sync_dest.Dest_file_fd
		data                   []byte
		ak, sk                 string
		propty                 int
		public                 bool
	)
	fsize, err = strconv.ParseInt(filesize, 10, 64)
	if err != nil {
		log.Errorf("parseint %s failed %v", filesize, err)
		return
	}
	blocksize, err = strconv.ParseInt(sync.c.BlockSize, 10, 64)
	if err != nil {
		log.Errorf("parseint block size %s failed %v", sync.c.BlockSize, err)
		return
	}

	filefd, err = sync.rsync_dest.Getfilefd(destname)
	if err != nil {
		log.Errorf("get filename %s  fd failed %v", destname, err)
		return
	}
	defer filefd.Close()
	e = blocksize

	ak, sk, propty, _, err = sync.ibucket.Getaksk(bucket)
	if err != nil {
		log.Errorf("get bucket %s ak sk fialed %v", bucket)
		return
	}
	if sync.ibucket.Public(propty, true) {
		public = true
	}

	if fsize > blocksize {
		for {
			frange := "bytes=" + strconv.FormatInt(b, 10) + "-" + strconv.FormatInt(e, 10)
			//log.Errorf("range= %s", frange)
			data, retcode, err = sync.rsync_efs.Getrangedata(bucket, filename, frange, ak, sk, dnsname, public)
			if err != nil {
				log.Errorf("get data from efs bucket % filename %s failed %v", bucket, filename, err)
				return
			}

			err = filefd.Writedata(data)
			if err != nil {
				log.Errorf("write file %s data failed %v", destname, err)
				return
			}
			if e > fsize || e == fsize {
				break
			}
			b = e + 1
			if e+blocksize > fsize {
				e = fsize
			} else {
				e = e + blocksize
			}
		}
	} else {
		data, retcode, err = sync.rsync_efs.Getdata(bucket, filename, ak, sk, dnsname, public)
		if err != nil {
			log.Errorf("get data from efs bucket % filename %s failed %v", bucket, filename, err)
			return
		}
		err = filefd.Writedata(data)
		if err != nil {
			log.Errorf("write file %s data failed %v", destname, err)
			return
		}
		return
	}

	return

}

func (sync *Rsync_efs) File_sync(method, bucket, filename, overflag, oldfsize, filesize string) (retcode int, err error) {
	var (
		dnsname string
	)
	if method == sync.c.UploadMethod || method == sync.c.UploadMkfile {
		dnsname, err, retcode = sync.ibucket.Getdnsname(bucket)
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", bucket, err)
			return
		}
		destname := "/" + dnsname + "/" + filename
		retcode, err = sync.File_copy_data(bucket, filename, destname, filesize, dnsname)
		if err != nil {
			log.Errorf("file sync copy data failed %v", err)
			return
		}

	} else if method == sync.c.DeleteMethod {
		dnsname, err, retcode = sync.ibucket.Getdnsname(bucket)
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", bucket, err)
			return
		}
		destname := "/" + dnsname + "/" + filename
		err = sync.rsync_dest.File_delete(destname)
		if err != nil {
			log.Errorf("file sync delete file %s  failed %v", destname, err)
			return
		}
	} else if method == sync.c.Mv {
		//mv bucket is srckey filename is destekey
		arr := strings.Split(bucket, ":")
		dnsname, err, retcode = sync.ibucket.Getdnsname(arr[0])
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", arr[0], err)
			return
		}
		srcname := "/" + dnsname + "/" + arr[1]

		arr1 := strings.Split(filename, ":")
		dnsname, err, retcode = sync.ibucket.Getdnsname(arr1[0])
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", arr1[0], err)
			return
		}
		destname := "/" + dnsname + "/" + arr1[1]
		err = sync.rsync_dest.File_mv(srcname, destname)
		if err != nil {
			log.Errorf("file sync mv %s to %s  failed %v", srcname, destname, err)
			return
		}

	} else if method == sync.c.Copy {
		//copy bucket is srckey filename is destekey
		arr := strings.Split(bucket, ":")
		dnsname, err, retcode = sync.ibucket.Getdnsname(arr[0])
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", arr[0], err)
			return
		}
		srcname := "/" + dnsname + "/" + arr[1]

		arr1 := strings.Split(filename, ":")
		dnsname, err, retcode = sync.ibucket.Getdnsname(arr1[0])
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", arr1[0], err)
			return
		}
		destname := "/" + dnsname + "/" + arr1[1]
		err = sync.rsync_dest.File_copy(srcname, destname)
		if err != nil {
			log.Errorf("file sync mv %s to %s  failed %v", srcname, destname, err)
			return
		}

	} else if method == sync.c.DeleteBucket {
		dnsname, err, retcode = sync.ibucket.Getdnsname(bucket)
		if err != nil {
			log.Errorf("get dnsname by bucket %s failed %v", bucket, err)
			return
		}
		deletedir := "/" + dnsname + "/"
		err = sync.rsync_dest.Del_dir(deletedir)
		if err != nil {
			log.Errorf("sync delete dir %s  failed %v", deletedir, err)
			return
		}

	} else {
		return
	}
	return

}
