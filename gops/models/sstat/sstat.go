package sstat

import (
	"database/sql"
	"errors"
	"github.com/astaxie/beego"
	_ "github.com/go-sql-driver/mysql"
)

type QPS struct {
	ID       int64
	Ctime    string
	StoreID  string
	Upload   int32
	Download int32
	Del      int32
}

type Throughput struct {
	ID      int64
	Ctime   string
	StoreID string
	Tpin    int32
	Tpout   int32
}

type Delay struct {
	ID       int64
	Ctime    string
	StoreID  string
	Upload   int32
	Download int32
	Del      int32
}

var db *sql.DB

//db, _ := sql.Open("mysql", "root:gosun.com@tcp(192.168.100.185:3306)/gops?charset=utf8")
func init() {
	dsn := beego.AppConfig.String("MysqlDSN")
	db, _ = sql.Open("mysql", dsn)
}

//store qps add
func QPSAdd(qps *QPS) (id int64, err error) {
	if qps == nil {
		err = errors.New("store qps add error, wrong qps data")
		return
	}

	sql := "insert into store_qps(ctime, storeid, upload, download, del) values(?, ?, ?, ?, ?)"
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	result, err := stmt.Exec(qps.Ctime, qps.StoreID, qps.Upload, qps.Download, qps.Del)
	if err != nil {
		return
	}

	id, err = result.LastInsertId()
	if err != nil || id == 0 {
		return
	}

	return
}

//store qps get by storeid and time offset
func QPSbyCon(storeid string, offset int32) (qpss []*QPS, err error) {
	if storeid == "" || offset == 0 {
		err = errors.New("store id or time offset error")
		return
	}

	sql := "select * from store_qps where storeid = ? order by ctime desc limit ?"

	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query(storeid, offset)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		qps := new(QPS)
		err = rows.Scan(&qps.ID, &qps.Ctime, &qps.StoreID, &qps.Upload, &qps.Download, &qps.Del)
		if err != nil {
			return
		}
		qpss = append(qpss, qps)
	}

	return
}

//store Throughput add
func ThroughputAdd(tp *Throughput) (id int64, err error) {
	if tp == nil {
		err = errors.New("store throughput add error, wrong throughput data")
		return
	}

	sql := "insert into throughput(ctime, storeid, tpin, tpout) values(?, ?, ?, ?)"
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	result, err := stmt.Exec(tp.Ctime, tp.StoreID, tp.Tpin, tp.Tpout)
	if err != nil {
		return
	}

	id, err = result.LastInsertId()
	if err != nil || id == 0 {
		return
	}

	return
}

//throughput get by storeid and time offset
func ThroughputbyCon(storeid string, offset int32) (tps []*Throughput, err error) {
	if storeid == "" || offset == 0 {
		err = errors.New("storeid or time offset error")
		return
	}

	sql := "select * from throughput where storeid = ? order by ctime desc limit ?"
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query(storeid, offset)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		tp := new(Throughput)
		err = rows.Scan(&tp.ID, &tp.Ctime, &tp.StoreID, &tp.Tpin, &tp.Tpout)
		if err != nil {
			return
		}
		tps = append(tps, tp)
	}

	return
}

//Store Delay add
func DelayAdd(delay *Delay) (id int64, err error) {
	if delay == nil {
		err = errors.New("store delay add error, wrong delay data")
		return
	}

	sql := "insert into store_delay(ctime, storeid, upload, download, del) values (?, ?, ?, ?, ?)"
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	result, err := stmt.Exec(delay.Ctime, delay.StoreID, delay.Upload, delay.Download, delay.Del)
	if err != nil {
		return
	}

	id, err = result.LastInsertId()
	if err != nil || id == 0 {
		return
	}

	return
}

//delay get by storeid and time offset
func DelaybyCon(storeid string, offset int32) (delays []*Delay, err error) {
	if storeid == "" || offset == 0 {
		err = errors.New("storeid or time offset error")
		return
	}

	sql := "select * from store_delay where storeid = ? order by ctime desc limit ?"
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query(storeid, offset)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		delay := new(Delay)
		err = rows.Scan(&delay.ID, &delay.Ctime, &delay.StoreID, &delay.Upload, &delay.Download, &delay.Del)
		if err != nil {
			return
		}
		delays = append(delays, delay)
	}

	return
}

func Add_QPS(storeid string, upload uint64, download uint64, del uint64, ctime string) {
	qps := &QPS{Ctime: ctime, StoreID: storeid,
		Upload: int32(upload), Download: int32(download), Del: int32(del)}
	_, err := QPSAdd(qps)
	if err != nil {
		beego.Error(err)
	}
}

func Add_Delay(storeid string, upload uint64, download uint64, del uint64, ctime string) {
	delay := &Delay{Ctime: ctime, StoreID: storeid,
		Upload: int32(upload), Download: int32(download), Del: int32(del)}
	_, err := DelayAdd(delay)
	if err != nil {
		beego.Error(err)
	}
}

func Add_Throughput(storeid string, tpin uint64, tpout uint64, ctime string) {
	tp := &Throughput{Ctime: ctime, StoreID: storeid,
		Tpin: int32(tpin), Tpout: int32(tpout)}
	_, err := ThroughputAdd(tp)
	if err != nil {
		beego.Error(err)
	}
}
