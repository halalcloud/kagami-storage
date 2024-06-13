package database

import (
	sql "database/sql"
	liberr "efs/libs/errors"
	"efs/libs/meta"
	"errors"
	"fmt"
	"time"

	log "efs/log/glog"

	_ "github.com/go-sql-driver/mysql"
)

func getcode(id int64) (codestring string) {
	var (
		i    int
		code []string
		t    string
	)

	a := 'a'
	for i = 0; i < 36; i++ {
		if i < 26 {
			code = append(code, fmt.Sprintf("%s", string(rune(i+int(a)))))
		} else {
			t := i - 26
			code = append(code, fmt.Sprintf("%d", t))
		}
	}

	i = 0
	for id > 0 {
		remainder := id % 36
		t = t + code[remainder]
		id = id / 36
		i++
	}

	codestring = t
	return
}

func shorturl(uid int) (dnsname string) {
	dnsname = getcode(int64(uid)) + getcode(time.Now().Unix())
	return

}

func AddBucket(bucketName, imgsource, key, keysecret, dnsname, userdnsname string, regionId,
	propety, replication, uid int) (err error) {
	var (
	//res sql.Result
	//	id  int64
	)
	/*
			if dnsname == "" {
				dnstmp := shorturl(uid)
				dnsname = dnstmp + dnssuffix
			}

		resdnsname = dnsname
	*/
	stmt, err1 := Db.Prepare("INSERT INTO bucket(userid,bucket_name,region_id,image_source,keyid,propety,dnsname,userdnsname,keysecret,replication,create_time)values(?,?,?,?,?,?,?,?,?,?,?)")
	if err1 != nil {
		//	fmt.Println("come here ------------------")
		log.Errorf("prepare database: %s", err1.Error())
		err = errors.New("insert database bucket failed")
		return
	}
	//fmt.Println("come here ------22222222222222222------------")
	if _, err = stmt.Exec(uid, bucketName, regionId, imgsource, key, propety, dnsname, userdnsname, keysecret, replication, time.Now().Format("2006-01-02 15:04:05")); err != nil {
		log.Errorf("add bucket exe database: %s", err.Error())
		return
	}
	/*
		if id, err = res.LastInsertId(); err != nil {
			log.Errorf("add bucket get lasetinsertid: %s", err.Error())
			return
		}
		dnstmp := shorturl(uid)
		dnsname := dnstmp + dnssuffix

		stmt, err1 = Db.Prepare("UPDATE bucket SET dnsname=? WHERE bucket_name=? and userid=?")
		if err1 != nil {
			log.Errorf("update propety prepare database: %s", err.Error())
			err = err1
			return
		}

		if _, err = stmt.Exec(dnsname, bucketName, uid); err != nil {
			log.Errorf("update propety exe database: %s", err.Error())
		}
	*/
	//	fmt.Println("come here ------33333333333333------------", id)
	return
}

func DeleteBucket(bucketName string, uid int) (err error) {
	var err1 error
	stmt, err1 := Db.Prepare("DELETE FROM `bucket` WHERE bucket_name=? and userid=? ")
	if err1 != nil {
		log.Errorf("delete database: %s", err1.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(bucketName, uid); err != nil {
		log.Errorf("delete bucket exe database: %s", err.Error())
	}
	return
}

func UpdatePropety(bucketName string, propety, uid int) (err error) {
	stmt, err1 := Db.Prepare("UPDATE bucket SET propety=? WHERE bucket_name=? and userid=?")
	if err1 != nil {
		log.Errorf("update propety prepare database: %s", err1.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(propety, bucketName, uid); err != nil {
		log.Errorf("update propety exe database: %s", err.Error())
	}
	return
}

func Setimgsource(bucketName, imgsource string, uid int) (err error) {
	stmt, err1 := Db.Prepare("UPDATE bucket SET image_source=? WHERE bucket_name=? and userid=?")
	if err1 != nil {
		log.Errorf("update propety prepare database: %s", err1.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(imgsource, bucketName, uid); err != nil {
		log.Errorf("update propety exe database: %s", err.Error())
	}
	return
}

func Setask(bucketName, ak, sk string, uid int) (err error) {
	stmt, err1 := Db.Prepare("UPDATE bucket SET keyid=?,keysecret=? WHERE bucket_name=? and userid=?")
	if err1 != nil {
		log.Errorf("update set ak sk prepare database: %s", err1.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(ak, sk, bucketName, uid); err != nil {
		log.Errorf("update propety exe database: %s", err1.Error())
	}
	return
}

func SetStyleDelimiter(bucketName, styleDelimiter string, uid int) (err error) {
	stmt, err1 := Db.Prepare("UPDATE bucket SET style_delimiter=? WHERE bucket_name=? and userid=?")
	if err1 != nil {
		log.Errorf("update set style_delimiter prepare database: %s", err1.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(styleDelimiter, bucketName, uid); err != nil {
		log.Errorf("update style_delimiter exe database: %s", err.Error())
	}
	return
}

func SetDPStyle(bucketName, dpstyle string, uid int) (err error) {
	stmt, err1 := Db.Prepare("UPDATE bucket SET dpstyle=? WHERE bucket_name=? and userid=?")
	if err1 != nil {
		log.Errorf("update set dpstyle prepare database: %s", err1.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(dpstyle, bucketName, uid); err != nil {
		log.Errorf("update dpstyle exe database: %s", err.Error())
	}
	return
}

func GetBucket(bucketName string, userid int) (imgsource, key, keysecret, create_time, dnsname, userdnsname string, regionId,
	propety, replication int, styledelimiter, dpstyle string, err error) {
	var (
		id                                                  string
		bucket_name                                         string
		styleDelimiter_null, dpstyle_null, userdnsname_null sql.NullString
		uid                                                 int
	)

	if err1 := Db.QueryRow("SELECT * FROM bucket WHERE bucket_name=? and userid=?", bucketName, userid).Scan(&id, &uid, &bucket_name,
		&regionId, &imgsource, &key, &propety, &dnsname, &userdnsname_null, &keysecret, &replication,
		&styleDelimiter_null, &dpstyle_null, &create_time); err1 != nil {

		log.Errorf("get bucket prepare database: %s bn:%s", err1.Error(), bucketName)
		if err1 == sql.ErrNoRows {
			err = liberr.ErrDestBucketNoExist
		} else {
			err = errors.New("database get bucket failed")
		}

		return
	}

	if styleDelimiter_null.Valid {
		styledelimiter = styleDelimiter_null.String
	} else {
		styledelimiter = ""
	}

	if dpstyle_null.Valid {
		dpstyle = dpstyle_null.String
	} else {
		dpstyle = ""
	}
	if userdnsname_null.Valid {
		userdnsname = userdnsname_null.String
	} else {
		userdnsname = ""
	}
	//if _, err = row.Scan(&id, &bucket_name, &regionId, &imgsource, &key, &propety, &keysecret, &create_time); err != nil {
	//log.Errorf("update propety exe database: %s", err.Error())
	//}
	return
}

func GetBucketBybnameak(bucketName, ak string) (imgsource, key, keysecret, create_time, dnsname, userdnsname string, regionId,
	propety, replication int, styledelimiter, dpstyle string, uid int, err error) {
	var (
		id                                                  string
		bucket_name                                         string
		styleDelimiter_null, dpstyle_null, userdnsname_null sql.NullString
	)

	if err1 := Db.QueryRow("SELECT * FROM bucket WHERE bucket_name=? and keyid=?", bucketName, ak).Scan(&id, &uid, &bucket_name,
		&regionId, &imgsource, &key, &propety, &dnsname, &userdnsname_null, &keysecret, &replication,
		&styleDelimiter_null, &dpstyle_null, &create_time); err1 != nil {

		log.Errorf("update propety prepare database: %s bn:%s", err1.Error(), bucketName)
		if err1 == sql.ErrNoRows {
			err = liberr.ErrDestBucketNoExist
		} else {
			err = errors.New("database get bucket failed")
		}

		return
	}

	if styleDelimiter_null.Valid {
		styledelimiter = styleDelimiter_null.String
	} else {
		styledelimiter = ""
	}

	if dpstyle_null.Valid {
		dpstyle = dpstyle_null.String
	} else {
		dpstyle = ""
	}
	if userdnsname_null.Valid {
		userdnsname = userdnsname_null.String
	} else {
		userdnsname = ""
	}

	//if _, err = row.Scan(&id, &bucket_name, &regionId, &imgsource, &key, &propety, &keysecret, &create_time); err != nil {
	//log.Errorf("update propety exe database: %s", err.Error())
	//}
	return
}

func GetBucketbydnsname(dnsname string) (bucketname, imgsource, key, keysecret, create_time string,
	regionId, propety, replication int, styledelimiter, dpstyle, userdnsname string, uid int, err error) {
	var (
		id                                                  string
		dns_name                                            string
		styleDelimiter_null, dpstyle_null, userdnsname_null sql.NullString
	)

	if err1 := Db.QueryRow("SELECT * FROM bucket WHERE dnsname=?", dnsname).Scan(&id, &uid, &bucketname, &regionId,
		&imgsource, &key, &propety, &dns_name, &userdnsname_null, &keysecret, &replication,
		&styleDelimiter_null, &dpstyle_null, &create_time); err1 != nil {
		log.Errorf("get bucket info by dnsname %s database: %s", dnsname, err1.Error())
		if err1 == sql.ErrNoRows {
			err = liberr.ErrDestBucketNoExist
		} else {
			err = errors.New("database get bucket by dnsname failed")
		}

		return
	}
	if styleDelimiter_null.Valid {
		styledelimiter = styleDelimiter_null.String
	} else {
		styledelimiter = ""
	}

	if dpstyle_null.Valid {
		dpstyle = dpstyle_null.String
	} else {
		dpstyle = ""
	}
	if userdnsname_null.Valid {
		userdnsname = userdnsname_null.String
	} else {
		userdnsname = ""
	}
	//if _, err = row.Scan(&id, &bucket_name, &regionId, &imgsource, &key, &propety, &keysecret, &create_time); err != nil {
	//log.Errorf("update propety exe database: %s", err.Error())
	//}
	return
}

func GetBucketbyuserdnsname(userdnsname string) (bucketname, imgsource, key, keysecret, create_time string,
	regionId, propety, replication int, styledelimiter, dpstyle, dnsname string, uid int, err error) {
	var (
		id string
		//userdns_name                                            string
		styleDelimiter_null, dpstyle_null, userdnsname_null sql.NullString
	)

	if err1 := Db.QueryRow("SELECT * FROM bucket WHERE userdnsname=?", userdnsname).Scan(&id, &uid, &bucketname, &regionId,
		&imgsource, &key, &propety, &dnsname, &userdnsname_null, &keysecret, &replication,
		&styleDelimiter_null, &dpstyle_null, &create_time); err1 != nil {
		log.Errorf("get bucket info by dnsname %s database: %s", dnsname, err1.Error())
		if err1 == sql.ErrNoRows {
			err = liberr.ErrDestBucketNoExist
		} else {
			err = errors.New("database get bucket by dnsname failed")
		}

		return
	}
	if styleDelimiter_null.Valid {
		styledelimiter = styleDelimiter_null.String
	} else {
		styledelimiter = ""
	}

	if dpstyle_null.Valid {
		dpstyle = dpstyle_null.String
	} else {
		dpstyle = ""
	}
	//if _, err = row.Scan(&id, &bucket_name, &regionId, &imgsource, &key, &propety, &keysecret, &create_time); err != nil {
	//log.Errorf("update propety exe database: %s", err.Error())
	//}
	return
}

func GetBucketbyUserid(uid int) (buckets []*meta.Bucket_item, err error) {
	var (
		styleDelimiter_null, dpstyle_null, userdnsname_null sql.NullString
	)

	stmt, err1 := Db.Prepare("SELECT * FROM bucket WHERE userid=?")
	if err1 != nil {
		log.Errorf("seleect *from uid %d error: %s", uid, err1.Error())
		err = err1
		return
	}

	rows, err1 := stmt.Query(uid)
	if err1 != nil {
		log.Errorf("sql query failed")
		if err1 == sql.ErrNoRows {
			err = liberr.ErrDestBucketNoExist
		} else {
			err = errors.New("database get bucket by dnsname failed")
		}
		return
	}
	var id string
	for rows.Next() {
		item := new(meta.Bucket_item)
		err1 = rows.Scan(&id, &item.Uid, &item.Bname, &item.RegionId, &item.Imgsource,
			&item.Key, &item.Propety, &item.Dnsname, &userdnsname_null, &item.Keysecret, &item.Replication,
			&styleDelimiter_null, &dpstyle_null, &item.Ctime)
		//		&item.Styledelimiter, &item.Dpstyle, &item.Ctime)
		if err1 != nil {
			log.Errorf("rows scan failed %v", err1)
			err = err1
			return
		}
		if styleDelimiter_null.Valid {
			item.Styledelimiter = styleDelimiter_null.String
		} else {
			item.Styledelimiter = ""
		}

		if dpstyle_null.Valid {
			item.Dpstyle = dpstyle_null.String
		} else {
			item.Dpstyle = ""
		}
		if userdnsname_null.Valid {
			item.UserDnsName = userdnsname_null.String
		} else {
			item.UserDnsName = ""
		}
		buckets = append(buckets, item)
	}
	return
}
