package oplog

import (
	"database/sql"
	"errors"
	"github.com/astaxie/beego"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"time"
)

type OpLog struct {
	Id       int64
	OpTime   string
	OpUId    int32
	OpDetail string
}

var db *sql.DB

func init() {
	dsn := beego.AppConfig.String("MysqlDSN")
	db, _ = sql.Open("mysql", dsn)
}

//oplog add
func Add(opl *OpLog) (id int64, err error) {
	if opl == nil {
		err = errors.New("oplog was nil")
		return
	}

	sql := `insert into oplog(op_time, op_uid, op_detail) values(?, ?, ?)`

	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}

	result, err := stmt.Exec(opl.OpTime, opl.OpUId, opl.OpDetail)
	if err != nil {
		return
	}

	id, err = result.LastInsertId()
	if err != nil || id == 0 {
		return
	}

	return
}

//oplog get all
func OpLogs(page, pageNum int32) (opls []OpLog, err error) {
	if page <= 0 || pageNum <= 0 {
		err = errors.New("page or pageNum error")
		return
	}
	pageOff := (page - 1) * pageNum

	sql := "select * from oplog order by op_time desc limit " + strconv.Itoa(int(pageOff)) + "," +
		strconv.Itoa(int(pageNum))
	rows, err := db.Query(sql)
	if err != nil {
		return
	}

	for rows.Next() {
		opl := OpLog{}
		err = rows.Scan(&opl.Id, &opl.OpTime, &opl.OpUId, &opl.OpDetail)
		if err != nil {
			return
		}
		opls = append(opls, opl)
	}

	return
}

//add oplog
func Add_OpLog(uid int32, detail string) {

	oplog := &OpLog{OpUId: uid, OpDetail: detail,
		OpTime: time.Now().Format("2006-01-02 15:04:05")}

	id, err := Add(oplog)
	if err != nil || id <= 0 {
		beego.Error("OpLog add error: " + "uid--" +
			strconv.Itoa(int(uid)) + "; detail--" + detail)
	}
}
