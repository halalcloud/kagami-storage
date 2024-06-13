package errors

const (
	RetSetRblStatusFailed     = 501
	RetCleanRblChildrenFailed = 502
	RetStartRblFailed         = 503
	RetRblAlreadyStart        = 504
	RetGetRblStatusFailed     = 505
)

var (
	ErrSetRblStatus    = Error(RetSetRblStatusFailed)
	ErrCleanRblChild   = Error(RetCleanRblChildrenFailed)
	ErrStartRblFailed  = Error(RetStartRblFailed)
	ErrRblAlreadyStart = Error(RetRblAlreadyStart)
	ErrGetRblStatus    = Error(RetGetRblStatusFailed)
)

type StoreInfo struct {
	MoveStatus    int
	MoveTotalData int64
	MoveData      int64
	UTime         int64
}

type VidInfo struct {
	DestVid     int64
	DestRockId  int64
	DestStoreId int64
	DestGroupId int64
	StoresInfo  map[int64]StoreInfo
}
