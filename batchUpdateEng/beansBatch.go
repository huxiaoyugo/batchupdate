package batchUpdateEng

import (
	"reflect"
	"sync"
)

type beanBatch struct {
	mu       sync.Mutex
	beans    []interface{}
	curIndex int
}

type BeanBatch interface {
	Init(... interface{})
	Next(int) ([]interface{}, bool)
	Clear()
}

func (this *beanBatch) Init(beans ...interface{}) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.convert2Slice(beans...)
}

func (this *beanBatch) Next(beanCount int) ([]interface{}, bool) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if beanCount <= 0 {
		return []interface{}{}, false
	}

	if this.curIndex >= len(this.beans) {
		return []interface{}{}, false
	}

	if this.curIndex+beanCount > len(this.beans) {
		resBeans := this.beans[this.curIndex:]
		this.curIndex = len(this.beans)
		return resBeans, true
	}

	resBeans := this.beans[this.curIndex : this.curIndex+beanCount]
	this.curIndex += beanCount

	return resBeans, true
}


func (this *beanBatch) Clear() {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.beans = this.beans[0:0]
	this.curIndex = 0
}

func (this *beanBatch) convert2Slice(beans ... interface{}) {
	this.beans = make([]interface{}, 0)
	for _, bean := range beans {
		sliceValue := reflect.Indirect(reflect.ValueOf(bean))
		if sliceValue.Kind() == reflect.Slice {
			size := sliceValue.Len()
			if size > 0 {
				for i := 0; i < size; i++ {
					this.beans = append(this.beans, sliceValue.Index(i).Interface())
				}
			}
		} else {
			this.beans = append(this.beans, bean)
		}
	}
}
