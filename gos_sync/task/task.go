package task

import (
	log "efs/log/glog"
	"encoding/json"

	"gopkg.in/mgo.v2"
)

type Task struct {
	mUrl    string
	session *mgo.Session
}

func New(murl string) (t *Task) {
	var (
		err error
	)

	t = new(Task)
	t.mUrl = murl

	t.session, err = mgo.Dial(t.mUrl)
	if err != nil {
		log.Fatalf("mgo dial err:%s\n", err)
	}
	t.session = t.session.Clone()
	return
}

func (t *Task) GetOne(db, collection, query string) (result string, err error) {
	var (
		q, r interface{}
		data []byte
	)

	if err = json.Unmarshal([]byte(query), &q); err != nil {
		log.Errorf("json decode query:%s err:%s\n", query, err)
		return
	}

	if err = t.session.DB(db).C(collection).Find(q).One(&r); err != nil {
		log.Errorf("get query:%s one err:%s\n", query, err)
		t.session.Refresh()
		return
	}

	if data, err = json.Marshal(r); err != nil {
		log.Errorf("json encode r:%+v err:%s\n", r, err)
		return
	}

	result = string(data)
	return
}

func (t *Task) GetN(db, collection, query string, n int) (results string, err error) {
	var (
		q    interface{}
		rs   []interface{}
		data []byte
	)

	if err = json.Unmarshal([]byte(query), &q); err != nil {
		log.Errorf("json decode query:%s err:%s\n", query, err)
		return
	}

	if err = t.session.DB(db).C(collection).Find(q).Sort("addtime").Limit(n).All(&rs); err != nil {
		log.Errorf("getn query:%s n err:%s\n", query, err)
		t.session.Refresh()
		return
	}

	if data, err = json.Marshal(rs); err != nil {
		log.Errorf("json encode rs:%+v err:%s\n", rs, err)
		return
	}

	results = string(data)
	return
}

func (t *Task) GetAll(db, collection, query string) (results string, err error) {
	var (
		q    interface{}
		rs   []interface{}
		data []byte
	)

	if err = json.Unmarshal([]byte(query), &q); err != nil {
		log.Errorf("json decode query:%s err:%s\n", query, err)
		return
	}

	if err = t.session.DB(db).C(collection).Find(q).Sort("addtime").All(&rs); err != nil {
		log.Errorf("getall query:%s all err:%s\n", query, err)
		t.session.Refresh()
		return
	}

	if data, err = json.Marshal(rs); err != nil {
		log.Errorf("json encode rs:%+v err:%s\n", rs, err)
		return
	}

	results = string(data)
	return
}

func (t *Task) Del(db, collection string, selector interface{}) (err error) {

	if err = t.session.DB(db).C(collection).Remove(selector); err != nil {
		log.Errorf("del selector:%+v err:%s\n", selector, err)
		t.session.Refresh()
		return
	}

	return
}

type FinishItem struct {
	Op      string
	Param   string
	AddTime int64
	Ekey    string
	Stat    string
}

func (t *Task) Insert(db, collection string, doc interface{}) (err error) {

	if err = t.session.DB(db).C(collection).Insert(doc); err != nil {
		log.Errorf("insert dos:%+v err:%s\n", doc, err)
		return
	}

	return
}
