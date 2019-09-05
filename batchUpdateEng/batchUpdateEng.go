package batchUpdateEng

import (
	"github.com/go-xorm/xorm"
	"fmt"
	"reflect"
	"strings"
	"strconv"
	"runtime"
	"github.com/mkideal/log"
	"errors"
)

// 一次update最多的对象个数
const DefaultMaxUpdateCount = 500

type BatchUpdateEng struct {
	eng        OrmEng      // 数据库引擎
	bOpt       BatchOption // 配置对象
	cols       []string    // 待更新的列
	beansBatch BeanBatch   // 分批处理待更新的beans
	pk         string      // 主键名称
	tableName  string      // 表明
}

func NewBatchUpdateEngine(eng *xorm.Engine, opts ...Option) *BatchUpdateEng {
	bue := &BatchUpdateEng{
		eng:        NewXormEngine(eng),
		beansBatch: &beanBatch{},
		bOpt:       apply(&opts),
	}
	if bue.bOpt.onceMaxCount == 0 {
		bue.bOpt.onceMaxCount = DefaultMaxUpdateCount
	}
	return bue
}

// 设置配置项
func (eng* BatchUpdateEng) SetOpt(opts ...Option) *BatchUpdateEng {
	for _, opFunc := range opts {
		opFunc(&eng.bOpt)
	}
	return eng
}

func (eng *BatchUpdateEng) Cols(cols ... string) *BatchUpdateEng {
	eng.cols = eng.cols[0:0]
	eng.cols = append(eng.cols, cols...)
	return eng
}

func (eng *BatchUpdateEng) Table(tableName string) *BatchUpdateEng {
	eng.tableName = tableName
	return eng
}

/*
	如果不主动指定主键，那么会自动根据待更新的对象获取,
	但前提是表结构在定义是必须使用xorm中的tag pk 进行标记。
	否则无法正常找到主键字段。
 */
func (eng *BatchUpdateEng) Pk(primaryKey string) *BatchUpdateEng {
	eng.pk = primaryKey
	return eng
}


func (eng *BatchUpdateEng) Update(beans ... interface{}) (affected int64, err error) {
	defer eng.clear()
	eng.beansBatch.Init(beans...)
	Try(func() {
		for {
			beanArr, has := eng.beansBatch.Next(eng.bOpt.onceMaxCount)
			if !has {
				break
			}
			var af int64
			af, err = eng.update(beanArr)
			if err != nil {
				break
			}
			affected += af
		}
	})
	return
}

func (eng *BatchUpdateEng) SetMaxUpdateCount(maxCount int) {
	eng.bOpt.onceMaxCount = maxCount
}

func (eng *BatchUpdateEng) GetMaxUpdateCount() int {
	return eng.bOpt.onceMaxCount
}

func (eng *BatchUpdateEng) update(beans []interface{}) (int64, error) {
	// 转换为批量update的sql语句
	sql, err := eng.createdBatchUpdateSQL(beans)
	if err != nil {
		return 0, err
	}

	result, err := eng.eng.Exec(sql)
	if err != nil {
		if eng.isShowLog() {
			log.Error("Error:%v", err)
		}
	}
	return result, err
}

func (eng *BatchUpdateEng) createdBatchUpdateSQL(beans []interface{}) (string, error) {
	var (
		pkVals string
	)

	valMaps, err := eng.getValMaps(beans)
	if err != nil {
		return "", err
	}

	if eng.pk == "" {
		return "", errors.New("pk is nil")
	}

	if eng.tableName == "" {
		return "", errors.New("table name is nil")
	}

	if len(eng.cols) == 0 {
		return "", errors.New("update cols is nil")
	}

	for pkVal := range valMaps {
		pkVals += fmt.Sprintf("%d,", pkVal)
	}
	pkVals = pkVals[0 : len(pkVals)-1]

	preSql := fmt.Sprintf("update %s set ", eng.tableName)

	colSql := ""
	for index, col := range eng.cols {
		sql := col + " = case " + eng.pk
		for k, val := range valMaps {
			sql += fmt.Sprintf(" when %d then '%v' ", k, (*val)[col])
		}
		sql += " end "
		if index != len(eng.cols)-1 {
			sql += ", "
		}
		colSql += sql
	}
	whereSql := fmt.Sprintf(" where %s in (%s)", eng.pk, pkVals)

	resultSql := preSql + colSql + whereSql

	if eng.isShowLog() {
		log.Info(resultSql)
	}

	return resultSql, nil
}

func (eng *BatchUpdateEng) getValMaps(beans []interface{}) (map[int]*map[string]interface{}, error) {
	valMaps := make(map[int]*map[string]interface{})
	var err error
	if len(beans) == 0 {
		return valMaps, errors.New("待更新的模型个数为0")
	}

	if eng.pk == "" {
		eng.pk, err = eng.eng.GetPrimaryKey(beans[0])
		if err != nil {
			return valMaps, err
		}
	}

	if eng.tableName == "" {
		eng.tableName = eng.eng.GetTableName(beans[0])
	}

	if len(eng.cols) == 0 {
		if eng.bOpt.isAutoUpdateAllCols {
			columns := eng.eng.Columns(beans[0])
			eng.cols = append(eng.cols, columns...)
		} else {
			return valMaps, errors.New("there is no update cols")
		}
	}

	for _, bean := range beans {
		sliceValue := reflect.Indirect(reflect.ValueOf(bean))
		if sliceValue.Kind() != reflect.Struct {
			return valMaps, errors.New("update bean is not struct")
		}
		pkVal, err := eng.getPrimaryKeyVal(bean)
		if err != nil {
			return valMaps, err
		}
		vMap := make(map[string]interface{}, 0)
		valMaps[pkVal] = &vMap

		// 获取每个字段的值
		for _, col := range eng.cols {
			vMap[col] = eng.getValue(bean, col)
		}
	}
	return valMaps, nil
}

func (eng *BatchUpdateEng) getPrimaryKeyVal(bean interface{}) (val int, err error) {

	if eng.pk == "" {
		eng.pk, err = eng.eng.GetPrimaryKey(bean)
		if err != nil {
			return 0, err
		}
	}
	if err != nil && eng.isShowLog() {
		log.Error("Error:%v", err)
	}
	return interfaceToInt(eng.getValue(bean, eng.pk))
}

func (eng *BatchUpdateEng) getValue(bean interface{}, key string) interface{} {

	if key == "" {
		if eng.isShowLog() {
			log.Warn("GetValue key is empty")
		}
		return nil
	}

	beanValue := reflect.ValueOf(bean)

	val := reflect.Indirect(beanValue)

	key = toCamelCase(key)
	return val.FieldByName(key).Interface()
}

func (eng *BatchUpdateEng) isShowLog() bool {
	return eng.bOpt.isShowLog
}

func (eng *BatchUpdateEng) clear() {
	eng.pk = ""
	eng.tableName = ""
	eng.beansBatch.Clear()
	eng.cols = make([]string, 0)
}

func toCamelCase(key string) string {

	res := ""
	arr := strings.Split(key, "_")
	for _, item := range arr {
		for index, char := range item {
			if index == 0 && char >= 97 && char <= 122 {
				res += string(char - 32)
			} else {
				res += string(char)
			}
		}
	}
	return res
}

func interfaceToInt(bean interface{}) (int, error) {

	switch reflect.TypeOf(bean).Kind() {
	case reflect.Int, reflect.Int64, reflect.Int16, reflect.Int32, reflect.Int8,
		reflect.Uint, reflect.Uint64, reflect.Uint16, reflect.Uint32, reflect.Uint8:
		val, err := strconv.Atoi(fmt.Sprintf("%v", bean))
		return val, err
	default:
		return 0, errors.New("目前支持主键为整形或者字符的")
	}
}

func Try(fn func()) (err error) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			switch typ := e.(type) {
			case error:
				err = typ
			case string:
				err = errors.New(typ)
			default:
				err = fmt.Errorf("%v", typ)
			}
			log.Error(fmt.Sprintf("==== STACK TRACE BEGIN ====\npanic: %v\n%s\n===== STACK TRACE END =====", err, string(buf)))
		}
	}()
	fn()
	return
}
