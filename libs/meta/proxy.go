package meta

//mkblk
type PMkblkRetOK struct {
	Ctx      string `json:"ctx"`
	Id       string `json:"id"`
	Checksum string `json:"checksum"`
	Crc32    int64  `json:"crc32"`
	Offset   int64  `json:"offset"`
	Host     string `json:"host"`
}

type PMkblkRetFailed struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

//bput
type PBputRetOK struct {
	Ctx      string `json:"ctx"`
	Checksum string `json:"checksum"`
	Crc32    int64  `json:"crc32"`
	Offset   int64  `json:"offset"`
	Host     string `json:"host"`
}

type PBputRetFailed struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

//mkfile
type PMkfileRetOK struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

type PMkfileRetFailed struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

//bucket rename
type PBRenameRetFailed struct {
	Error string `json:"error"`
}

//bucket delete
type PBDeleteRetFailed struct {
	Error string `json:"error"`
}

//buckert create
type PBCreateRetFailed struct {
	Error string `json:"error"`
}

//bucket list
type PBListRetOK struct {
	Buckets []string `json:"buckets"`
}

//bucket stat
type PBStatRetOK struct {
	Exist bool `json:"exist"`
}

type PBStatRetFailed struct {
	Error string `json:"error"`
}

type PBListRetFailed struct {
	Error string `json:"error"`
}

type FItem struct {
	Key      string `json:"key"`
	PutTime  int64  `json:"putTime"`
	Hash     string `json:"hash"`
	FSize    int64  `json:"fsize"`
	MimeType string `json:"mimeType"`
	Customer string `json:"customer"`
}

//list
type PFListRetOK struct {
	Marker         string   `json:"marker"`
	CommonPrefixes []string `json:"commonPrefixes"`
	FItems         []FItem  `json:"items"`
}

type PFListFailed struct {
	Error string `json:"error"`
}

//chgm
type PFStatChgFailed struct {
	Error string `json:"error"`
}

//copy
type PFCopyFailed struct {
	Error string `json:"error"`
}

//move
type PFMvFailed struct {
	Error string `json:"error"`
}

//stat
type PFStatRetOK struct {
	FSize           int64  `json:"fsize"`
	Hash            string `json:"hash"`
	MimeType        string `json:"mimeType"`
	PutTime         int64  `json:"putTime"`
	DeleteAfterDays int    `json:"deleteAfterDays"`
}

type PFStatFailed struct {
	Error string `json:"error"`
}

//upload
type PFUploadRetOK struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

type PFUploadFailed struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

//file delete
type PFDelFailed struct {
	Error string `json:"error"`
}

//batch
type PFBatchItem struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
}
