package db

import (
	"kagamistoreage/bigfile_callbak/conf"

	log "kagamistoreage/log/glog"
	//"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	_database_callbak = "mkfilecallbak"
	_table_callbakurl = "callbakurl"
)

type Mgo_op struct {
	session *mgo.Session
	c       *conf.Config
}

type Callbakinfo struct {
	Ekey       string
	Callbakurl string
	Utime      int64
}

func NewSession(c *conf.Config) (mgo_op *Mgo_op, err error) {
	/*
		var (
			connaddr string
		)
		connaddr = fmt.Sprintf("mongodb://%s:%s@%s", c.MongoUserName, c.MongoPasswd, c.MongoAddr)
		//	fmt.Println(connaddr)
	*/
	mgo_op = new(Mgo_op)
	mgo_op.c = c

	mgo_op.session, err = mgo.Dial(c.MongoAddr)
	if err != nil {
		log.Errorf("connect to mongodb addr %s failed(%v)", c.MongoAddr, err)
	}

	return
}

func (op *Mgo_op) Insert(ekey, callbakurl string, callbaktimeout int64) (err error) {
	session := op.session.Clone()
	defer session.Close()
	collection := session.DB(_database_callbak).C(_table_callbakurl)
	info := &Callbakinfo{ekey, callbakurl, callbaktimeout}
	err = collection.Insert(info)
	if err != nil {
		log.Errorf("insert ekey failed (%v)", err)
		return
	}
	return
}

func (op *Mgo_op) Get(ekey string) (callbakurl string, err error) {
	var (
		info Callbakinfo
	)
	session := op.session.Clone()
	defer session.Close()
	collection := session.DB(_database_callbak).C(_table_callbakurl)
	err = collection.Find(bson.M{"ekey": ekey}).One(&info)
	if err != nil {
		log.Errorf("remove ekey failed(%v)", ekey, err)
	}
	callbakurl = info.Callbakurl
	return
}

func (op *Mgo_op) Del(ekey string) (err error) {
	session := op.session.Clone()
	defer session.Close()
	collection := session.DB(_database_callbak).C(_table_callbakurl)
	err = collection.Remove(bson.M{"ekey": ekey})
	if err != nil {
		log.Errorf("remove ekey failed(%v)", ekey, err)
	}
	return
}

func (op *Mgo_op) List() (infos []Callbakinfo, err error) {
	session := op.session.Clone()
	defer session.Close()
	collection := session.DB(_database_callbak).C(_table_callbakurl)
	err = collection.Find(nil).All(&infos)
	if err != nil {
		log.Errorf("list failed(%v)", err)
		return
	}

	return

}
