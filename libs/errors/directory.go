package errors

const (
	// hbase
	RetHBase              = 30100
	RetHBasePoolExhausted = 30101
	// id
	RetIdNotAvailable = 30200
	// store
	RetStoreNotAvailable = 30300
	// zookeeper
	RetZookeeperDataError = 30400

	RetHavenohelthvolume = 30500

	RetDestBucketNoExist = 30600

	RetSrcBucketNoExist = 30700

	RetDestBucketExist       = 30800
	RetMkfileDatalenNotMatch = 30900
)

var (
	// hbase
	ErrHBase              = Error(RetHBase)
	ErrHBasePoolExhausted = Error(RetHBasePoolExhausted)
	// id
	ErrIdNotAvailable = Error(RetIdNotAvailable)
	// store
	ErrStoreNotAvailable = Error(RetStoreNotAvailable)
	// zookeeper
	ErrZookeeperDataError = Error(RetZookeeperDataError)
	//dispatcher
	ErrHavenohelthvolume = Error(RetHavenohelthvolume)

	ErrDestBucketNoExist = Error(RetDestBucketNoExist)

	ErrSrcBucketNoExist = Error(RetSrcBucketNoExist)

	ErrDestBucketExist       = Error(RetDestBucketExist)
	ErrMkfileDatalenNotMatch = Error(RetMkfileDatalenNotMatch)
)
