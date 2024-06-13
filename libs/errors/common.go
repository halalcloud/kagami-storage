package errors

const (
	RetOK                 = 1
	RemoveLinkOK          = 2
	RetServiceUnavailable = 65533
	RetParamErr           = 65534
	RetInternalErr        = 65535

	//database failed
	RetDatabaseErr = 65536

	RetBucketNotService    = 70000
	RetProxyNotService     = 70001
	RetDirectoryNotService = 70002

	RetAuthacessIoErr = 70100

	// needle
	RetNeedleExist = 5000

	//parameter
	RetParameterFailed = 9000

	//multipart upload have no this block id
	RetHavenoBlockId  = 701
	RetOffsetNotMatch = 9101
)

var (
	// common
	ErrParam              = Error(RetParamErr)
	ErrInternal           = Error(RetInternalErr)
	ErrServiceUnavailable = Error(RetServiceUnavailable)

	ErrNeedleExist = Error(RetNeedleExist)

	ErrDatabase = Error(RetDatabaseErr)

	//parameter
	ErrParameterFailed = Error(RetParameterFailed)

	ErrHavenoBlockId  = Error(RetHavenoBlockId)
	ErrOffsetNotMatch = Error(RetOffsetNotMatch)
)
