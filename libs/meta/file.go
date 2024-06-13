package meta

type File struct {
	Filename        string  `json:"filename"`
	Filesize        string  `json:"filesize"`
	Key             []int64 `json:"key"` //slice upload
	Sha1            string  `json:"sha1"`
	Mine            string  `json:"mine"`
	Status          int32   `json:"status"`
	MTime           int64   `json:"update_time"`
	DeleteAftertime int64   `json:"deleteAfterDays"` //timeout Unixtime
}
