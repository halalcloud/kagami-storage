package main

// import (
// 	"bytes"
// 	"crypto/sha1"
// 	"kagamistoreage/egc/conf"
// 	"kagamistoreage/egc/efs"
// 	"kagamistoreage/libs/errors"
// 	"kagamistoreage/libs/meta"
// 	"encoding/base64"
// 	"encoding/hex"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	log "github.com/golang/glog"
// 	"hash/crc32"
// 	"io"
// 	"io/ioutil"
// 	"net/http"
// 	"os"
// 	"path"
// 	"strconv"
// 	"strings"
// 	"time"
// )

// type server struct {
// 	efs *efs.Efs
// 	c   *conf.Config
// }

// func main() {
// 	var (
// 		configFile string
// 		regular    string
// 		err        error
// 		// data       []byte
// 		respOK meta.PBListRetOK
// 		c      *conf.Config
// 	)

// 	flag.StringVar(&configFile, "c", "./egc.toml", " set directory config file path")

// 	if c, err = conf.NewConfig(configFile); err != nil {
// 		panic(err)
// 	}
// 	var s = &server{}
// 	s.c = c
// 	s.efs = efs.New(c)

// 	regular = "bucket_trash_\\w+"
// 	if respOK.Buckets, err = s.efs.BucketList(regular); err == nil {
// 		for _, bn := range respOK.Buckets {

// 		}
// 		return
// 	} else {
// 		println("error!!!!!!!!!")
// 		return
// 	}
// }

// func main() {
// 	var (
// 		data       []byte
// 		configFile string
// 		flist      *meta.PFListRetOK
// 		err        error
// 		c          *conf.Config
// 	)

// 	flag.StringVar(&configFile, "c", "./egc.toml", " set directory config file path")

// 	if c, err = conf.NewConfig(configFile); err != nil {
// 		panic(err)
// 	}

// 	var s = &server{}
// 	s.c = c
// 	s.efs = efs.New(c)

// 	bucket := "tmp"
// 	ekey := base64.URLEncoding.EncodeToString([]byte(bucket))
// 	fmt.Println(ekey)
// 	if flist, err = s.efs.List(ekey, "1", "", ""); err == nil {
// 		if flist != nil {
// 			if data, err = json.Marshal(flist); err == nil {
// 				fmt.Println(data)
// 			}
// 		}
// 	}
// }
