package main

import (
	log "kagamistoreage/fuds2kagamistoreage/libs/glog"

	"ecloud_gosdk.v1/conf"
	"ecloud_gosdk.v1/ecloud"
	"ecloud_gosdk.v1/ecloudcli"
)

type GOSSyncer struct {
	fileBasePath string
	bucketAKSK   map[string][]string
	upHost       string
	mgHost       string
}

func (gs *GOSSyncer) sync(taskChan chan *logTask) {
	var (
		err error
	)

	for v := range taskChan {
		switch v.method {
		case "UPLOAD":
			err = gs.uploadFile(v.bucket, v.key, gs.fileBasePath+"/"+v.bucket+"/"+v.key, v.filesize)
			if err != nil {
				log.Errorf("upload file err:%s bucket:%s key:%s\n", err.Error(), v.bucket, v.key)
				continue
			}
		case "DELETE":
			err = gs.deleteFile(v.bucket, v.key)
			if err != nil {
				log.Errorf("delete file err:%s bucket:%s key:%s\n", err.Error(), v.bucket, v.key)
				continue
			}
		case "RENAME":
			err = gs.renameFile(v.bucket, v.key, v.mvkey)
			if err != nil {
				log.Errorf("rename file err:%s bucket:%s key:%s mvkey:%s\n",
					err.Error(), v.bucket, v.key, v.mvkey)
				continue
			}
		default:
			log.Errorf("error task method:%s\n", v.method)
			continue
		}
	}

	return
}

type PutRet struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

func (gs *GOSSyncer) uploadFile(bucket, key, filepath string, filesize int64) (err error) {
	zone := 0
	tc := ecloud.Config{AccessKey: gs.bucketAKSK[bucket][0], SecretKey: gs.bucketAKSK[bucket][1]}

	conf.Zones[zone].UpHosts[0] = gs.upHost
	conf.Zones[zone].MgHosts[0] = gs.mgHost

	c := ecloud.New(zone, &tc)
	policy := &ecloud.PutPolicy{
		Scope:      bucket,
		Expires:    3600,
		InsertOnly: 0,
	}
	token := c.MakeUptoken(policy)

	uploader := ecloudcli.NewUploader(zone, nil)

	var ret PutRet
	if filesize > 4*1024*1024 {
		err = uploader.RputFile(nil, &ret, token, key, filepath, nil)
	} else {
		err = uploader.PutFile(nil, &ret, token, key, filepath, nil)
	}
	if err != nil {
		log.Errorf("upload err:%s bucket:%s key:%s filepath:%s filesize:%d\n",
			err.Error(), bucket, key, filepath, filesize)
		return
	}

	log.Infof("upload ok bucket:%s key:%s filepath:%s\n",
		bucket, key, filepath)
	return
}

func (gs *GOSSyncer) deleteFile(bucket, key string) (err error) {
	tc := ecloud.Config{AccessKey: gs.bucketAKSK[bucket][0], SecretKey: gs.bucketAKSK[bucket][1]}

	zone := 0
	conf.Zones[zone].UpHosts[0] = gs.upHost
	conf.Zones[zone].MgHosts[0] = gs.mgHost

	c := ecloud.New(zone, &tc)
	p := c.Bucket(bucket)

	if err = p.Delete(nil, key); err != nil {
		log.Errorf("delete err:%s bucket:%s key:%s\n",
			err.Error(), bucket, key)
		return
	}

	log.Infof("delete ok bucket:%s key:%s\n",
		bucket, key)
	return
}

func (gs *GOSSyncer) renameFile(bucket, key, movekey string) (err error) {
	tc := ecloud.Config{AccessKey: gs.bucketAKSK[bucket][0], SecretKey: gs.bucketAKSK[bucket][1]}

	zone := 0
	conf.Zones[zone].UpHosts[0] = gs.upHost
	conf.Zones[zone].MgHosts[0] = gs.mgHost

	c := ecloud.New(zone, &tc)
	p := c.Bucket(bucket)

	if err = p.Move(nil, key, movekey); err != nil {
		log.Errorf("move err:%s, bucket:%s key:%s movekey:%s\n",
			err.Error(), bucket, key, movekey)
		return
	}

	log.Infof("rename ok bucket:%s key:%s movekey:%s\n",
		bucket, key, movekey)
	return
}
