package errors

const (
	// common
	RetRespOK       = 200
	RetUrlBad       = 400
	RetServerFailed = 599
	RetResNoExist   = 612
	RetResExist     = 614

	//--- 待删
	RetAuthFailed     = 401
	RetBucketNotExist = 404
	RetBucketExist    = 405
	//---

	// upload
	RetFileTooLarge      = 413
	RetCallbackFailed    = 579
	RetSliceUploadFailed = 702

	//down
	RetRangeBad = 416

	//batch
	RetPartialFailed = 298
)

var (
	// common
	ErrUrlBad       = Error(RetUrlBad)
	ErrServerFailed = Error(RetServerFailed)
	ErrResNoExist   = Error(RetResNoExist)
	ErrResExist     = Error(RetResExist)

	//--- 待删
	ErrAuthFailed     = Error(RetAuthFailed)
	ErrBucketNotExist = Error(RetBucketNotExist)
	ErrBucketExist    = Error(RetBucketExist)

	// upload
	ErrFileTooLarge      = Error(RetFileTooLarge)
	ErrCallBackFailed    = Error(RetCallbackFailed)
	ErrSliceUploadFailed = Error(RetSliceUploadFailed)

	//down
	ErrRangeBad = Error(RetRangeBad)

	//batch
	ErrPartialFailed = Error(RetPartialFailed)
)
