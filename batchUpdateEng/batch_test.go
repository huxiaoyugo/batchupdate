package batchUpdateEng

import (
	"testing"
	"fmt"
	"github.com/go-xorm/xorm"
	"cw_bg/utils"
	"github.com/qianlnk/log"
	"cw_bg/app/models/DBModels"
)

func MysqlEng() (*xorm.Engine, error) {
	dbname := "washcar_dev_db"
	user := "testuser"
	password :="mEMz/JFFnA=="
	host := "59.110.27.156:3306"
	defaultSid := "simple_bg"
	password = string(utils.DescryptBase64(password, defaultSid))

	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true", user, password, host, dbname)

	engine, err := xorm.NewEngine("mysql", dataSourceName)
	return engine, err
}



type StuTb struct {
	Id int  `xorm:"INT(11) "`
	Name string
	UserCount int
}

func TestNewBatchUpdateEngine(t *testing.T) {

	cwEng, err := MysqlEng()

	if err != nil {
		fmt.Println(err)
		return
	}
	eng := NewBatchUpdateEngine(cwEng,WithShowLog(true))

	af , err := eng.Cols("user_count").SetOpt(WithShowLog(true)).Update([]interface{}{&models.DailyPaperTb{Id:4920,UserCount:106},&models.DailyPaperTb{Id:4921,UserCount:10}})
	if err != nil {
		log.Error(err)
	}
	fmt.Println(af)

	af , err = eng.Pk("id").SetOpt(WithShowLog(true),WithAutoUpdateAllCols(false)).Update([]interface{}{&StuTb{Id:4920,UserCount:106},&StuTb{Id:4921,UserCount:10}})
	if err != nil {
		log.Error(err)
	}
	fmt.Println(af)
}
