package rmanger

import (
	"kagamistoreage/bucket/database"
	"kagamistoreage/bucket/global"
	"kagamistoreage/libs/errors"
	"sync"
	"time"

	log "kagamistoreage/log/glog"
)

type Regions struct {
	data  map[string]*Item
	rlock *sync.RWMutex
}

type Item struct {
	RegionName string
	Id         int64
	timeout    int64
}

func New() (rs *Regions) {
	var (
		rnames map[int64]string
		name   string
		id     int64
		err    error
	)
	rs = new(Regions)
	rs.data = make(map[string]*Item)
	rnames = make(map[int64]string)
	rs.rlock = new(sync.RWMutex)

	if rnames, err = database.GetRegion(); err != nil {
		return
	}

	for id, name = range rnames {
		item := new(Item)
		item.RegionName = name
		item.Id = id
		item.timeout = time.Now().Unix()
		rs.data[name] = item
	}
	return
}

func (rs *Regions) getitemfromid(id int) (ritem *Item, err error) {
	var (
		flag       int
		item       *Item
		regionname string
	)

	flag = 0
	rs.rlock.RLock() //r lock
	for regionname, ritem = range rs.data {
		if ritem.Id == int64(id) {
			if (time.Now().Unix() - ritem.timeout) > int64(global.Timeout) {
				flag = 1           //timeout reget from sql
				rs.rlock.RUnlock() //r unlock
				goto timeout
			}
			flag = 1
			break
		}
	}
	rs.rlock.RUnlock() //r unlock
	if flag == 1 {
		return
	}
timeout:

	regionname, err = database.GetRegionname(id)
	if err != nil {
		if flag == 1 {
			rs.rlock.Lock() //w lock
			delete(rs.data, regionname)
			rs.rlock.Unlock() //w unlock
		}
		return
	}
	rs.rlock.Lock() //w lock
	if flag == 1 {
		ritem.Id = int64(id)
		ritem.RegionName = regionname
		ritem.timeout = time.Now().Unix()
		rs.rlock.Unlock() //w unlock
		return
	}
	item = new(Item)
	item.Id = int64(id)
	item.RegionName = regionname
	item.timeout = time.Now().Unix()
	rs.data[regionname] = item
	rs.rlock.Unlock() //w unlock
	ritem = item

	return

}

func (rs *Regions) getitemfromname(rname string) (ritem *Item, err error) {
	var (
		ok   bool
		id   int
		item *Item
		flag int
	)
	rs.rlock.RLock() //r lock
	ritem, ok = rs.data[rname]
	rs.rlock.RUnlock() //r unlock
	if ok {
		if (time.Now().Unix() - ritem.timeout) > int64(global.Timeout) {
			flag = 1 //timeout reget from sql
			goto timeout
		}
		return
	}
timeout:

	id, err = database.GetRegionid(rname)
	if err != nil {
		if flag == 1 {
			rs.rlock.Lock() //w lock
			delete(rs.data, rname)
			rs.rlock.Unlock() //w unlock
		}
		return
	}
	rs.rlock.Lock() //w lock
	if flag == 1 {
		ritem.Id = int64(id)
		ritem.RegionName = rname
		rs.rlock.Unlock() //w unlock
		return
	}
	item = new(Item)
	item.Id = int64(id)
	item.RegionName = rname
	rs.rlock.Unlock() //w unlock

	ritem = item

	return
}

func (rs *Regions) AddRegion(RegionName string) (err error) {
	var (
		id int64
	)

	if _, err = rs.getitemfromname(RegionName); err == nil {
		err = errors.ErrBucketExist
		log.Errorf("region %s is exsit", RegionName)
		return
	}

	if id, err = database.AddRegion(RegionName); err != nil {
		return
	}

	item := new(Item)
	item.RegionName = RegionName
	item.Id = id
	rs.rlock.Lock() //w lock
	rs.data[RegionName] = item
	rs.rlock.Unlock() //w unlock
	return
}

func (rs *Regions) GetRegionid(RegionName string) (id int, err error) {
	var (
		item *Item
	)

	if item, err = rs.getitemfromname(RegionName); err != nil {
		err = errors.ErrBucketNotExist
		log.Errorf("region %s is exsit", RegionName)
		return
	}

	id = int(item.Id)
	return

}

func (rs *Regions) GetRegionname(id int) (rname string, err error) {
	var (
		item *Item
	)

	if item, err = rs.getitemfromid(id); err != nil {
		err = errors.ErrBucketExist
		log.Errorf("region id %d is exsit", id)
		return
	}

	rname = item.RegionName
	return

}
