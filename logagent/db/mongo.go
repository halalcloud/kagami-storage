package db

import (
	"kagamistoreage/logagent/conf"

	log "kagamistoreage/log/glog"
	//	"fmt"

	"gopkg.in/mgo.v2"
	//"gopkg.in/mgo.v2/bson"
)

type Mgo_op struct {
	session *mgo.Session
	c       *conf.Config
}

type Db_loginfo struct {
	Op      string
	Param   string
	Ekey    string
	AddTime int64
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

func (op *Mgo_op) Insert(ekey, param, op_method string, addtime int64) (err error) {
	session := op.session.Clone()
	defer session.Close()
	collection := session.DB(op.c.MongoDatabase).C(op.c.MongoTable)
	info := &Db_loginfo{op_method, param, ekey, addtime}
	err = collection.Insert(info)
	if err != nil {
		log.Errorf("insert ekey failed (%v)", err)
		return
	}
	return
}
