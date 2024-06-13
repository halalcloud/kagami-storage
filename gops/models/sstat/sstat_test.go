package sstat

import (
	"testing"
)

/*
func Test_StoreQPSAdd(t *testing.T) {
	sqps := &StoreQPS{Ctime: "2016-10-09 13:13:13", StoreID: "s001", Upload: 5, Download: 10, Del: 15}
	id, err := StoreQPSAdd(sqps)
	if err != nil {
		t.Error(err)
	}
	t.Log(id)
}
*/

func Test_Add_QPS(t *testing.T) {
	Add_QPS("s002", 10, 50, 100)
}

/*
func Test_ThroughputAdd(t *testing.T) {
	tp := &Throughput{Ctime: "2016-10-09 12:12:12", StoreID: "s001", Tpin: 500, Tpout: 3000}
	id, err := ThroughputAdd(tp)
	if err != nil {
		t.Error(err)
	}
	t.Log(id)
}
*/

func Test_Add_Throughput(t *testing.T) {
	Add_Throughput("s002", 200, 600)
}

/*
func Test_DelayAdd(t *testing.T) {
	delay := &Delay{Ctime: "2016-10-12 11:11:11", StoreID: "s001", Upload: 300, Download: 500, Del: 1000}
	id, err := DelayAdd(delay)
	if err != nil {
		t.Error(err)
	}
	t.Log(id)
}
*/

func Test_Add_Delay(t *testing.T) {
	Add_Delay("s002", 100, 300, 700)
}
