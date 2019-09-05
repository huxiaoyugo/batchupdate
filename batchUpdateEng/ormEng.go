package batchUpdateEng

import (
	"github.com/go-xorm/xorm"
	"github.com/pkg/errors"
)

type OrmEng interface {
	GetTableName(bean interface{})(string)
	GetPrimaryKey(bean interface{})(string, error)
	Columns(bean interface{})([]string)
	Exec(rawSql string)(int64, error)
}


type XormEngine struct {
	eng *xorm.Engine
}

func NewXormEngine(eng *xorm.Engine) *XormEngine{
	return &XormEngine{
		eng:eng,
	}
}
func (xorm *XormEngine) GetTableName(bean interface{}) string {
	return xorm.eng.TableInfo(bean).Name
}


func (xorm *XormEngine) GetPrimaryKey(bean interface{}) (string,error) {
	pks := xorm.eng.TableInfo(bean).PrimaryKeys
	if len(pks) <= 0 {
		return "", errors.New("该对象没有主键")
	}
	if len(pks) > 1 {
		return "", errors.New("该对象为复合主键，目前只支持单主键")
	}
	return pks[0], nil
}

func (xorm *XormEngine)Columns(bean interface{}) (cols []string) {
	columns := xorm.eng.TableInfo(bean).Columns()
	for _, item := range columns {
		cols = append(cols, item.Name)
	}
	return
}

func (xorm *XormEngine) Exec(rawSql string) (int64, error) {
	res, err := xorm.eng.Exec(rawSql)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}