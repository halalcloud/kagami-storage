package meta

// StoreRet

const (
	Dispatcher_score   = 0
	Dispatcher_polling = 1
)

type StoreRets struct {
	SRets []StoreRet `json:"storeRet"`
}

type StoreRet struct {
	Ret  int    `json:"ret"`
	Host string `json:"host"`
	Err  error  `json:"err"`
}

type StoreS struct {
	Stores []string
}
type SliceResponse struct {
	Ret   int        `json:"ret"`
	MTime int64      `json:"update_time"`
	Sha1  string     `json:"sha1"`
	Mine  string     `json:"mine"`
	Fsize string     `json:"fsize"`
	Keys  []int64    `json:"keys"`
	Res   []Response `json:"res"`
}

// Response
type Response struct {
	Ret             int      `json:"ret"`
	Key             int64    `json:"key"`
	Cookie          int32    `json:"cookie"`
	Vid             int32    `json:"vid"`
	Stores          []string `json:"stores"`
	MTime           int64    `json:"update_time"`
	Sha1            string   `json:"sha1"`
	Mine            string   `json:"mine"`
	Fsize           string   `json:"fsize"`
	OFSize          string   `json:"ofsize"`
	Offset          int64    `json:"offset"`
	DeleteAftertime int      `json:"deleteAftertime"`
}

type Item struct {
	Key             string  `json:"key"`
	PutTime         int64   `json:"putTime"`
	Hash            string  `json:"hash"`
	Fsize           int64   `json:"fsize"`
	MimeType        string  `json:"mimeType"`
	Customer        string  `json:"customer"`
	Deleteaftertime int64   `json:"deleteaftertime,omitempty"`
	Keys            []int64 `json:"keys"`
}

type FileList struct {
	Marker         string   `json:"marker"`
	CommonPrefixes []string `json:"commonPrefixes"`
	Items          []Item   `json:"items"`
}
type FileListResponse struct {
	Ret   int      `json:"ret"`
	Flist FileList `json:flist`
}

type ExpireItem struct {
	Key    string `json:"key"`
	Expire int64  `json:"expire"`
}

type ExpireList struct {
	Marker string       `json:"marker"`
	Items  []ExpireItem `json:"items"`
}

//destroy list
type DestroyListResponse struct {
	Ret        int            `json:"ret"`
	Trash_flag bool           `json:"trash_flag"`
	Marker     string         `json:"marker"`
	DList      []*DestroyFile `json:"dlist"`
}
type DestroyFile struct {
	FileName   string       `json:"filename"`
	Keys       []int64      `json:"keys"`
	FileNeedle *DFileNeedle `json:"fileneedle"`
}
type DFileNeedle struct {
	Key    int64    `json:"key"`
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}

//destroy
type DestroyFileResponse struct {
	Ret int `json:"ret"`
}

//destroy expire
type DestroyExpireResponse struct {
	Ret int `json:"ret"`
}

type BucketCreatResponse struct {
	Ret int `json:"ret"`
}
type BucketRenameResponse struct {
	Ret int `json:"ret"`
}
type BucketDeleteResponse struct {
	Ret int `json:"ret"`
}
type BucketDestroyResponse struct {
	Ret int `json:"ret"`
}
type BucketListResponse struct {
	Ret  int      `json:"ret"`
	List []string `json:"list"`
}
type BucketStatResponse struct {
	Ret   int  `json:"ret"`
	Exist bool `json:"exist"`
}

type CleanTimeoutResponse struct {
	Ret       int    `json:"ret"`
	Failfiles string `json:"failfiles"`
}
