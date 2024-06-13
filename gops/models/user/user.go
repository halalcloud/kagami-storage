package user

import (
	"database/sql"
	"errors"
	"github.com/astaxie/beego"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type User struct {
	ID         int32
	Acount     string
	Password   string
	Name       string
	Role       int32
	Stat       int32
	IsAlarm    int32
	Mail       string
	Phone      string
	QQ         string
	Remark     string
	LastLogin  string
	CreateTime string
}

var LoginUsers map[string]string = make(map[string]string)
var db *sql.DB

func init() {
	dsn := beego.AppConfig.String("MysqlDSN")
	db, _ = sql.Open("mysql", dsn)
}

//user get all
func Users(page, pageNum int32) (users []User, err error) {
	if page <= 0 || pageNum <= 0 {
		err = errors.New("page or pageNum error")
		return
	}

	pageOff := (page - 1) * pageNum
	sql := "select * from user limit " + strconv.Itoa(int(pageOff)) + "," + strconv.Itoa(int(pageNum))
	rows, err := db.Query(sql)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		user := User{}
		err = rows.Scan(&user.ID, &user.Acount, &user.Password, &user.Name, &user.Role,
			&user.Stat, &user.IsAlarm, &user.Mail, &user.Phone, &user.QQ,
			&user.Remark, &user.LastLogin, &user.CreateTime)
		if err != nil {
			return
		}
		users = append(users, user)
	}

	return
}

//user add
func Add(user *User) (id int32, err error) {
	if user == nil {
		err = errors.New("add user error, wrong user data")
		return
	}

	sql := `insert into user(acount, password, name, role, stat, is_alarm,mail, phone, qq ,
		remark, last_login, create_time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	result, err := stmt.Exec(user.Acount, user.Password, user.Name, user.Role, user.Stat,
		user.IsAlarm, user.Mail, user.Phone, user.QQ, user.Remark, user.LastLogin,
		user.CreateTime)
	if err != nil {
		return
	}

	tid, err := result.LastInsertId()
	id = int32(tid)
	if err != nil || id == 0 {
		return
	}

	return
}

//user get by Acount
func UserbyAcount(acount string) (user *User, err error) {
	if acount == "" {
		err = errors.New("acount was empty")
		return
	}

	sql := "select * from user where acount = ?"

	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}

	row := stmt.QueryRow(acount)

	user = &User{}
	err = row.Scan(&user.ID, &user.Acount, &user.Password, &user.Name, &user.Role,
		&user.Stat, &user.IsAlarm, &user.Mail, &user.Phone, &user.QQ, &user.Remark,
		&user.LastLogin, &user.CreateTime)
	if err != nil {
		return
	}

	return
}

//user get by con
func UsersbyCon(con map[string]string) (users []User, err error) {
	if con == nil {
		err = errors.New("user get conditon was nil")
		return
	}

	var conArr []interface{}
	sql := "select * from user where "
	l := len(con)
	i := 1
	for k, v := range con {
		conArr = append(conArr, "%"+v+"%")
		sql += k + " like " + "? "
		if i < l {
			sql += "or "
		}
		i++
	}

	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}

	rows, err := stmt.Query(conArr...)
	if err != nil {
		return
	}

	for rows.Next() {
		user := User{}
		err = rows.Scan(&user.ID, &user.Acount, &user.Password, &user.Name, &user.Role,
			&user.Stat, &user.IsAlarm, &user.Mail, &user.Phone, &user.QQ, &user.Remark,
			&user.LastLogin, &user.CreateTime)
		if err != nil {
			return
		}
		users = append(users, user)
	}

	return
}

//user is exist
func UserisExist(con map[string]string) (isExist bool, err error) {
	if con == nil {
		err = errors.New("user con was nil")
		return
	}

	var conArr []interface{}
	sql := "select * from user where "
	l := len(con)
	i := 1
	for k, v := range con {
		conArr = append(conArr, v)
		sql += k + " = " + "? "
		if i < l {
			sql += "and "
		}
		i++
	}
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}

	rows, err := stmt.Query(conArr...)
	if err != nil {
		return
	}

	if rows.Next() {
		isExist = true
	}

	return
}

//user update by id
func UpdatebyID(data map[string]string, id int32) (rs bool, err error) {
	if data == nil || id == 0 {
		err = errors.New("user update data was nil or id was 0")
		return
	}

	var dataArr []interface{}
	sql := "update user set "
	i := 1
	l := len(data)
	for k, v := range data {
		dataArr = append(dataArr, v)
		sql += k + " = " + "? "
		if i < l {
			sql += ", "
		}
		i++
	}
	sql += "where id = " + strconv.Itoa(int(id))
	stmt, err := db.Prepare(sql)
	if err != nil {
		return
	}
	result, err := stmt.Exec(dataArr...)
	if err != nil {
		return
	}

	ra, err := result.RowsAffected()
	if err != nil || ra == 0 {
		return
	}

	rs = true
	return
}

//user delete by id
func DeletebyID(id int32) (rs bool, err error) {
	if id == 0 {
		err = errors.New("user delete id was 0")
		return
	}

	sql := "delete from user where id = " + strconv.Itoa(int(id))

	result, err := db.Exec(sql)
	if err != nil {
		return
	}

	ra, err := result.RowsAffected()
	if err != nil || ra == 0 {
		return
	}

	rs = true
	return
}
