package bmanger

import (
	"kagamistoreage/bucket/database"
	"kagamistoreage/libs/errors"

	//"fmt"
	"kagamistoreage/bucket/global"
	//"net/url"
	"kagamistoreage/libs/meta"
	"sync"
	"time"

	log "kagamistoreage/log/glog"
)

type Buckets struct {
	data  map[int]*User_item
	block *sync.RWMutex
}

type Item struct {
	uid            int
	bname          string
	regionId       int
	keysecret      string
	key            string
	imgsource      string
	propety        int
	ctime          string
	dnsname        string
	userdnsname    string
	replication    int
	styledelimiter string
	dpstyle        string
	timeout        int64
}

type User_item struct {
	timeout int64
	items   []*Item
}

func New() (bs *Buckets) {
	bs = new(Buckets)
	bs.data = make(map[int]*User_item)
	bs.block = new(sync.RWMutex)
	return
}

func (b *Buckets) get_item_from_cache(bucketname string,
	userid int) (item *Item) {
	var (
		ok       bool
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[userid]
	if !ok {
		return nil
	} else {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				item = tmp_item
				break
			}
		}
	}

	return
}
func (b *Buckets) delete_cache_item(bucketname string, userid int) {
	var (
		ok, find bool
		uitem    *User_item
		tmp_item *Item
		index    int
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[userid]
	if !ok {
		return
	} else {
		for index, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				find = true
				break
			}
		}
	}
	if find {
		uitem.items = append(uitem.items[:index], uitem.items[index+1:]...)
	}
	return
}

func (b *Buckets) getcacheitem_by_dnsname(dnsname string) (item *Item) {
	var (
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock

	for _, uitem = range b.data {
		for _, tmp_item = range uitem.items {
			if tmp_item.dnsname == dnsname {
				item = tmp_item
				break
			}
		}
		if item != nil {
			break
		}
	}
	return
}

func (b *Buckets) getcacheitem_by_userdnsname(userdnsname string) (item *Item) {
	var (
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock

	for _, uitem = range b.data {
		for _, tmp_item = range uitem.items {
			if tmp_item.userdnsname == userdnsname {
				item = tmp_item
				break
			}
		}
		if item != nil {
			break
		}
	}
	return
}
func (b *Buckets) getcacheitem_by_ak_bname(bucketname, ak string) (item *Item) {
	var (
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	for _, uitem = range b.data {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname && tmp_item.key == ak {
				item = tmp_item
				break
			}
		}
		if item != nil {
			break
		}
	}

	return
}

func (b *Buckets) add_item_cache(item *Item) {
	if item == nil {
		return
	}
	b.block.Lock() //w lock
	defer b.block.Unlock()
	uitem, ok := b.data[item.uid]
	if ok {
		uitem.items = append(uitem.items, item)
		uitem.timeout = time.Now().Unix()
	} else {
		var uitems *User_item
		uitems = new(User_item)
		uitems.items = append(uitems.items, item)
		uitems.timeout = time.Now().Unix()
		b.data[item.uid] = uitems
	}
}

func (b *Buckets) getbitembyakbname(bucketname string, ak string) (ritem *Item, err error) {
	var (
		item         *Item
		flag, userid int

		regionId                    int
		keysecret                   string
		key                         string
		imgsource                   string
		propety, replication        int
		styledelimiter, dpstyle     string
		ctime, dnsname, userdnsname string
	)
	flag = 0
	item = b.getcacheitem_by_ak_bname(bucketname, ak)
	if item != nil {
		if (time.Now().Unix() - item.timeout) > int64(global.Timeout) {
			flag = 1 //timeout reget from sql
		} else {
			ritem = item
			return
		}
	}

	if flag == 0 {
		item = new(Item)
	}

	imgsource, key, keysecret, ctime, dnsname, userdnsname, regionId, propety,
		replication, styledelimiter, dpstyle, userid, err = database.GetBucketBybnameak(bucketname, ak)
	if err != nil {
		if flag == 1 {
			b.delete_cache_item(item.bname, item.uid)
		}
		return
	}
	if flag == 1 {
		b.block.Lock() //w lock
		defer b.block.Unlock()
	}
	item.uid = userid
	item.imgsource = imgsource
	item.key = key
	item.keysecret = keysecret
	item.ctime = ctime
	item.regionId = regionId
	item.propety = propety
	item.dnsname = dnsname
	item.userdnsname = userdnsname
	item.bname = bucketname
	item.replication = replication
	item.styledelimiter = styledelimiter
	item.dpstyle = dpstyle
	item.timeout = time.Now().Unix()

	if flag == 0 {
		b.add_item_cache(item)
	}

	//w unlock

	ritem = item
	return
}

func (b *Buckets) getbitem(bucketname string, userid int) (ritem *Item, err error) {
	var (
		item *Item
		flag int

		regionId                    int
		keysecret                   string
		key                         string
		imgsource                   string
		propety, replication        int
		styledelimiter, dpstyle     string
		ctime, dnsname, userdnsname string
	)
	flag = 0
	item = b.get_item_from_cache(bucketname, userid)
	if item != nil {
		if (time.Now().Unix() - item.timeout) > int64(global.Timeout) {
			flag = 1 //timeout reget from sql
		} else {
			ritem = item
			return
		}
	}

	if flag == 0 {
		item = new(Item)
	}

	imgsource, key, keysecret, ctime, dnsname, userdnsname, regionId, propety,
		replication, styledelimiter, dpstyle, err = database.GetBucket(bucketname, userid)
	if err != nil {
		if flag == 1 {
			b.delete_cache_item(bucketname, userid)
		}
		return
	}
	if flag == 1 {
		b.block.Lock() //w lock
		defer b.block.Unlock()
	}
	item.uid = userid
	item.imgsource = imgsource
	item.key = key
	item.keysecret = keysecret
	item.ctime = ctime
	item.regionId = regionId
	item.propety = propety
	item.dnsname = dnsname
	item.userdnsname = userdnsname
	item.bname = bucketname
	item.replication = replication
	item.styledelimiter = styledelimiter
	item.dpstyle = dpstyle
	item.timeout = time.Now().Unix()

	if flag == 0 {
		b.add_item_cache(item)
	}

	//w unlock

	ritem = item
	return
}

func (b *Buckets) getbitembydnsname(dnsname string) (ritem *Item, err error) {
	var (
		//	ok   bool
		item      *Item
		flag, uid int

		regionId                           int
		keysecret                          string
		key                                string
		imgsource, bucketname, userdnsname string
		propety, replication               int
		styledelimiter, dpstyle            string
		ctime                              string
	)
	flag = 0
	item = b.getcacheitem_by_dnsname(dnsname)
	if item != nil {
		if (time.Now().Unix() - item.timeout) > int64(global.Timeout) {
			flag = 1
		} else {
			ritem = item
			return
		}

	}

	if flag == 0 {
		item = new(Item)
	}
	bucketname, imgsource, key, keysecret, ctime, regionId, propety,
		replication, styledelimiter, dpstyle, userdnsname, uid,
		err = database.GetBucketbydnsname(dnsname)
	if err != nil {
		if flag == 1 {
			b.delete_cache_item(item.bname, item.uid)
		}
		return
	}
	if flag == 1 {
		b.block.Lock() //w lock
		defer b.block.Unlock()
	}
	item.uid = uid
	item.imgsource = imgsource
	item.key = key
	item.keysecret = keysecret
	item.ctime = ctime
	item.regionId = regionId
	item.propety = propety
	item.dnsname = dnsname
	item.userdnsname = userdnsname
	item.bname = bucketname
	item.replication = replication
	item.styledelimiter = styledelimiter
	item.dpstyle = dpstyle
	item.timeout = time.Now().Unix()

	if flag == 0 {
		b.add_item_cache(item)
	}

	ritem = item
	return
}

func (b *Buckets) getbitembyuserdnsname(userdnsname string) (ritem *Item, err error) {
	var (
		//	ok   bool
		item      *Item
		flag, uid int

		regionId                       int
		keysecret                      string
		key                            string
		imgsource, bucketname, dnsname string
		propety, replication           int
		styledelimiter, dpstyle        string
		ctime                          string
	)
	flag = 0
	item = b.getcacheitem_by_userdnsname(userdnsname)
	if item != nil {
		if (time.Now().Unix() - item.timeout) > int64(global.Timeout) {
			flag = 1
		} else {
			ritem = item
			return
		}

	}

	if flag == 0 {
		item = new(Item)
	}
	bucketname, imgsource, key, keysecret, ctime, regionId, propety,
		replication, styledelimiter, dpstyle, dnsname, uid,
		err = database.GetBucketbyuserdnsname(userdnsname)
	if err != nil {
		if flag == 1 {
			b.delete_cache_item(item.bname, item.uid)
		}
		return
	}
	if flag == 1 {
		b.block.Lock() //w lock
		defer b.block.Unlock()
	}
	item.uid = uid
	item.imgsource = imgsource
	item.key = key
	item.keysecret = keysecret
	item.ctime = ctime
	item.regionId = regionId
	item.propety = propety
	item.dnsname = dnsname
	item.userdnsname = userdnsname
	item.bname = bucketname
	item.replication = replication
	item.styledelimiter = styledelimiter
	item.dpstyle = dpstyle
	item.timeout = time.Now().Unix()

	if flag == 0 {
		b.add_item_cache(item)
	}

	ritem = item
	return
}

/*
Uid            int    `json:"uid"`

	Bname          string `json:"bname"`
	RegionId       int    `json:"regionid"`
	Keysecret      string `json:"keysecret"`
	Key            string `json:"key"`
	Imgsource      string `json:"imgsource"`
	Propety        int    `json:"propety"`
	Ctime          string `json:"ctime"`
	Dnsname        string `json:"dnsname"`
	Replication    int    `json:"replication"`
	Styledelimiter string `json:"styledelimiter"`
	Dpstyle        string `json:"dpstyle"`
*/
func Res_copy_buckets(bs []*Item) (buckets []*meta.Bucket_item) {
	for _, item := range bs {
		bucket := new(meta.Bucket_item)
		bucket.Uid = item.uid
		bucket.Bname = item.bname
		bucket.RegionId = item.regionId
		bucket.Keysecret = item.keysecret
		bucket.Key = item.key
		bucket.Imgsource = item.imgsource
		bucket.Propety = item.propety
		bucket.Ctime = item.ctime
		bucket.Dnsname = item.dnsname
		bucket.UserDnsName = item.userdnsname
		bucket.Replication = item.replication
		bucket.Styledelimiter = item.styledelimiter
		bucket.Dpstyle = item.dpstyle

		buckets = append(buckets, bucket)
	}
	return
}

func Get_copynew_buckets(buckets []*meta.Bucket_item) (bs []*Item) {
	for _, bucket := range buckets {
		item := new(Item)
		item.uid = bucket.Uid
		item.bname = bucket.Bname
		item.regionId = bucket.RegionId
		item.keysecret = bucket.Keysecret
		item.key = bucket.Key
		item.imgsource = bucket.Imgsource
		item.propety = bucket.Propety
		item.ctime = bucket.Ctime
		item.dnsname = bucket.Dnsname
		item.userdnsname = bucket.UserDnsName
		item.replication = bucket.Replication
		item.styledelimiter = bucket.Styledelimiter
		item.dpstyle = bucket.Dpstyle
		item.timeout = time.Now().Unix()
		bs = append(bs, item)
	}
	return
}

func Get_copy_buckets(buckets []*meta.Bucket_item, bs []*Item) (resbs []*Item) {
	var have bool
	for _, bucket := range buckets {
		for _, item := range bs {
			if bucket.Bname == item.bname {
				item.uid = bucket.Uid
				item.bname = bucket.Bname
				item.regionId = bucket.RegionId
				item.keysecret = bucket.Keysecret
				item.key = bucket.Key
				item.imgsource = bucket.Imgsource
				item.propety = bucket.Propety
				item.ctime = bucket.Ctime
				item.dnsname = bucket.Dnsname
				item.userdnsname = bucket.UserDnsName
				item.replication = bucket.Replication
				item.styledelimiter = bucket.Styledelimiter
				item.dpstyle = bucket.Dpstyle
				item.timeout = time.Now().Unix()
				have = true
				break
			}
		}
		if !have {
			item := new(Item)
			item.uid = bucket.Uid
			item.bname = bucket.Bname
			item.regionId = bucket.RegionId
			item.keysecret = bucket.Keysecret
			item.key = bucket.Key
			item.imgsource = bucket.Imgsource
			item.propety = bucket.Propety
			item.ctime = bucket.Ctime
			item.dnsname = bucket.Dnsname
			item.userdnsname = bucket.UserDnsName
			item.replication = bucket.Replication
			item.styledelimiter = bucket.Styledelimiter
			item.dpstyle = bucket.Dpstyle
			item.timeout = time.Now().Unix()
			bs = append(bs, item)
		}
		have = false
	}
	resbs = bs
	return
}

func (b *Buckets) getbucketsByuserid(uid int) (buckets []*meta.Bucket_item, err error) {
	var (
		flag  int
		uitem *User_item
	)
	flag = 0
	b.block.Lock() //w lock
	uitem, ok := b.data[uid]
	b.block.Unlock()
	if ok {
		if (time.Now().Unix() - uitem.timeout) > int64(global.Timeout) {
			flag = 1
		} else {
			buckets = Res_copy_buckets(uitem.items)
			return
		}
	}

	buckets, err = database.GetBucketbyUserid(uid)
	if err != nil {
		return
	}
	if flag == 0 {
		uitem = new(User_item)
	}

	b.block.Lock()
	defer b.block.Unlock()
	if flag == 0 {
		uitem.items = Get_copynew_buckets(buckets)
	} else {
		uitem.items = uitem.items[:0]
		uitem.items = Get_copy_buckets(buckets, uitem.items)
	}
	uitem.timeout = time.Now().Unix()
	b.data[uid] = uitem
	return
}

func (b *Buckets) Bcreate(bucketName, key, keysecret,
	imgsource, dnsname, userdnsname string, regionId, propety, replication, userid int) (err error) {
	var (
		item *Item
	)

	item, err = b.getbitem(bucketName, userid)
	if err != nil && err != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err)
		err = errors.ErrDatabase
		return
	}
	if item != nil {
		err = errors.ErrBucketExist
		log.Errorf("bucketname %s uid %d is exsit", bucketName, userid)
		return
	}

	if err = database.AddBucket(bucketName, imgsource, key, keysecret, dnsname, userdnsname, regionId, propety, replication, userid); err != nil {
		log.Errorf("database create buget name %s failed,err:%s", bucketName, err.Error())
		err = errors.ErrDatabase
		//fmt.Println("come here ---------bcreate----")
		return
	}
	//fmt.Println("come here ---------bcreate-2222---")
	item = new(Item)
	item.bname = bucketName
	item.regionId = regionId
	item.propety = propety
	item.keysecret = keysecret
	item.key = key
	item.imgsource = imgsource
	item.dnsname = dnsname
	item.userdnsname = userdnsname
	item.replication = replication
	item.ctime = time.Now().Format("2006-01-02 15:04:05")
	item.timeout = time.Now().Unix()
	b.add_item_cache(item)
	return

}

func (b *Buckets) Bget(bucketName string, uid int) (img_source, key, keysecret, create_time, dnsname, userDnsName string, propety,
	regionId, replication int, styledelimiter, dpstyle string, err error) {
	var (
		item *Item
	)

	item, err = b.getbitem(bucketName, uid)
	if err != nil && err != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketName, uid)
		return
	}
	img_source = item.imgsource
	key = item.key
	keysecret = item.keysecret
	propety = item.propety
	regionId = item.regionId
	create_time = item.ctime
	dnsname = item.dnsname
	userDnsName = item.userdnsname
	replication = item.replication
	styledelimiter = item.styledelimiter
	dpstyle = item.dpstyle
	return
}

func (b *Buckets) Bgetbydnsname(dnsname string) (bucketname, img_source, key, keysecret, create_time string, propety,
	regionId, replication int, styledelimiter, dpstyle, userDnsName string, uid int, err error) {
	var (
		item *Item
	)

	item, err = b.getbitembydnsname(dnsname)
	if err != nil && err != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("dnsname %s is not exist", dnsname)
		return
	}
	img_source = item.imgsource
	key = item.key
	keysecret = item.keysecret
	propety = item.propety
	regionId = item.regionId
	create_time = item.ctime
	bucketname = item.bname
	replication = item.replication
	userDnsName = item.userdnsname
	styledelimiter = item.styledelimiter
	dpstyle = item.dpstyle
	uid = item.uid
	return
}

func (b *Buckets) BgetByuserdnsname(userdnsname string) (bucketname, img_source, key, keysecret, create_time string, propety,
	regionId, replication int, styledelimiter, dpstyle, dnsname string, uid int, err error) {
	var (
		item *Item
	)

	item, err = b.getbitembyuserdnsname(userdnsname)
	if err != nil && err != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("dnsname %s is not exist", dnsname)
		return
	}
	img_source = item.imgsource
	key = item.key
	keysecret = item.keysecret
	propety = item.propety
	regionId = item.regionId
	create_time = item.ctime
	bucketname = item.bname
	replication = item.replication
	dnsname = item.dnsname
	styledelimiter = item.styledelimiter
	dpstyle = item.dpstyle
	uid = item.uid
	return
}

/*
func (b *Buckets) delbitem(bucketname string) {
	b.block.Lock()
	defer b.block.Unlock()
	delete(b.data, bucketname)
}
*/

func (b *Buckets) Bdel(bucketname string, uid int) (err error) {
	var item *Item
	item, err = b.getbitem(bucketname, uid)
	if err != nil && err != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketname, uid)
		return
	}
	// delete bucket data
	err = database.DeleteBucket(bucketname, uid)
	if err == nil {
		b.delete_cache_item(bucketname, uid)
	} else {
		log.Errorf("delete bucketname %d uid %d failed %v", bucketname, uid, err)
		return
	}
	return

}

func (b *Buckets) setimgsource(bucketname, imgsource string, uid int) {

	var (
		ok       bool
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[uid]
	if !ok {
		return
	} else {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				tmp_item.imgsource = imgsource
				break
			}
		}
	}

	return

}

func (b *Buckets) setpropety(bucketname string, propety, uid int) {

	var (
		ok       bool
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[uid]
	if !ok {
		return
	} else {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				tmp_item.propety = propety
				break
			}
		}
	}
	return
}

func (b *Buckets) setask(bucketname string, ak, sk string, uid int) {
	var (
		ok       bool
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[uid]
	if !ok {
		return
	} else {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				tmp_item.keysecret = sk
				tmp_item.key = ak
				break
			}
		}
	}

	return

}

func (b *Buckets) setStyleDelimiter(bucketname, styleDelimiter string, uid int) {
	var (
		ok       bool
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[uid]
	if !ok {
		return
	} else {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				tmp_item.styledelimiter = styleDelimiter
				break
			}
		}
	}

	return
}

func (b *Buckets) setDPStyle(bucketname, dpstyle string, uid int) {
	var (
		ok       bool
		uitem    *User_item
		tmp_item *Item
	)
	b.block.RLock()         //r lock
	defer b.block.RUnlock() //r unlock
	uitem, ok = b.data[uid]
	if !ok {
		return
	} else {
		for _, tmp_item = range uitem.items {
			if tmp_item.bname == bucketname {
				tmp_item.dpstyle = dpstyle
				break
			}
		}
	}

	return

}

func (b *Buckets) Bsetimgsource(bucketname, imgsource string, uid int) (err error) {
	item, err1 := b.getbitem(bucketname, uid)
	if err1 != nil && err1 != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err1)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketname, uid)
		return
	}
	err = database.Setimgsource(bucketname, imgsource, uid)
	if err == nil {
		b.setimgsource(bucketname, imgsource, uid)
	}
	return
}

func (b *Buckets) Bsetpropety(bucketname string, propety int64, uid int) (err error) {
	item, err1 := b.getbitem(bucketname, uid)
	if err1 != nil && err1 != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err1)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketname, uid)
		return
	}
	err = database.UpdatePropety(bucketname, int(propety), uid)
	if err == nil {
		b.setpropety(bucketname, int(propety), uid)
	}
	return
}

func (b *Buckets) Bsetask(bucketname string, ak, sk string, uid int) (err error) {
	item, err1 := b.getbitem(bucketname, uid)
	if err1 != nil && err1 != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err1)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketname, uid)
		return
	}
	err = database.Setask(bucketname, ak, sk, uid)
	if err == nil {
		b.setask(bucketname, ak, sk, uid)
	}
	return
}

func (b *Buckets) BSetStyleDelimiter(bucketname, styleDelimiter string, uid int) (err error) {
	item, err1 := b.getbitem(bucketname, uid)
	if err1 != nil && err1 != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err1)
		err = errors.ErrDatabase
		return
	}
	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketname, uid)
		return
	}
	err = database.SetStyleDelimiter(bucketname, styleDelimiter, uid)
	if err == nil {
		b.setStyleDelimiter(bucketname, styleDelimiter, uid)
	}
	return
}

func (b *Buckets) BSetDPStyle(bucketname, dpstyle string, uid int) (err error) {
	item, err1 := b.getbitem(bucketname, uid)
	if err1 != nil && err1 != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err1)
		err = errors.ErrDatabase
		return
	}

	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s uid %d is not exist", bucketname, uid)
		return
	}
	err = database.SetDPStyle(bucketname, dpstyle, uid)
	if err == nil {
		b.setStyleDelimiter(bucketname, dpstyle, uid)
	}
	return
}

func (b *Buckets) BgetByak(bucketname, ak string) (binfo *meta.Bucket_item, err error) {

	item, err1 := b.getbitembyakbname(bucketname, ak)
	if err1 != nil && err1 != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err1)
		err = errors.ErrDatabase
		return
	}

	if item == nil {
		err = errors.ErrBucketNotExist
		log.Errorf("bucketname %s ak %s is not exist", bucketname, ak)
		return
	}

	bucket := new(meta.Bucket_item)
	bucket.Uid = item.uid
	bucket.Bname = item.bname
	bucket.RegionId = item.regionId
	bucket.Keysecret = item.keysecret
	bucket.Key = item.key
	bucket.Imgsource = item.imgsource
	bucket.Propety = item.propety
	bucket.Ctime = item.ctime
	bucket.Dnsname = item.dnsname
	bucket.UserDnsName = item.userdnsname
	bucket.Replication = item.replication
	bucket.Styledelimiter = item.styledelimiter
	bucket.Dpstyle = item.dpstyle
	binfo = bucket

	return

}

func (b *Buckets) BgetByUserid(uid int) (buckets []*meta.Bucket_item, err error) {
	buckets, err = b.getbucketsByuserid(uid)
	if err != nil && err != errors.ErrDestBucketNoExist {
		log.Errorf("sql op failed %v", err)
		err = errors.ErrDatabase
		return
	}
	return
}
