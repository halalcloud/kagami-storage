package almrec

import (
	"database/sql"
	"errors"
	"github.com/astaxie/beego"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type Almrec struct {
	ID          int64
	AlarmDetail string
	ReceiveUID  string
	CreateTime  string
}

var db *sql.DB

func init() {
	dsn := beego.AppConfig.String("MysqlDSN")
	db, _ = sql.Open("mysql", dsn)
}

//almrec get all
func Almrecs(page, pageNum int32) (almrecs []Almrec, err error) {
	if page <= 0 || pageNum <= 0 {
		err = errors.New("page or pageNum error")
		return
	}
	pageOff := (page - 1) * pageNum

	sql := "select * from almrec order by create_time desc limit " + strconv.Itoa(int(pageOff)) + ", " +
		strconv.Itoa(int(pageNum))
	rows, err := db.Query(sql)
	if err != nil {
		return
	}

	for rows.Next() {
		almrec := Almrec{}
		err = rows.Scan(&almrec.ID, &almrec.AlarmDetail, &almrec.ReceiveUID, &almrec.CreateTime)
		if err != nil {
			return
		}
		almrecs = append(almrecs, almrec)
	}

	return
}

//almrec add
func AlmrecAdd(ar *Almrec) (id int64, err error) {
	if ar == nil {
		err = errors.New("almrec add error, wrong almrec data")
		return
	}

	sql := "insert into almrec(alarm_detail, receive_uid, create_time) values(?, ?, ?)"
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}

	result, err := stmt.Exec(ar.AlarmDetail, ar.ReceiveUID, ar.CreateTime)
	if err != nil {
		return
	}

	id, err = result.LastInsertId()
	if err != nil || id == 0 {
		return
	}

	return
}

func Add_Almrec(ad string, ctime string, mail string) {
	ar := &Almrec{AlarmDetail: ad, ReceiveUID: mail,
		CreateTime: ctime}
	id, err := AlmrecAdd(ar)
	if err != nil || id == 0 {
		beego.Error(err)
	}

	return
}
