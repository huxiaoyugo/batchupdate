package batchUpdateEng


type BatchOption struct {
	onceMaxCount  int
	isShowLog  bool
	isAutoUpdateAllCols  bool
}

type Option func(option *BatchOption)

func apply(opts *[]Option) BatchOption {
	op := BatchOption{}
	for _, opFunc := range *opts {
		opFunc(&op)
	}
	return op
}

func WithOnceMaxCount(maxCount int) Option{
	return func(option *BatchOption) {
		if maxCount <= 0 {
			option.onceMaxCount = DefaultMaxUpdateCount
		} else {
			option.onceMaxCount = maxCount
		}
	}
}

func WithShowLog(isShow bool) Option{
	return func(option *BatchOption) {
		option.isShowLog = isShow
	}
}

func WithAutoUpdateAllCols(autoAll bool) Option {
	return func(option *BatchOption) {
		option.isAutoUpdateAllCols = autoAll
	}
}