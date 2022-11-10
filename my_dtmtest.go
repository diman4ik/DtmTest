package main

import (
	"fmt"
	"log"
	"time"
	"strings"
	"database/sql"

	"github.com/dtm-labs/client/dtmcli"
	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/dtm-labs/client/dtmcli/logger"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"github.com/lithammer/shortuuid/v3"
	
	_ "github.com/go-sql-driver/mysql" // register mysql driver
	_ "github.com/lib/pq"              // register postgres driver
)


// busi address
const myBusiAPI = "/api/my_busi"
const myBusiPort = 8082

var myBusi = fmt.Sprintf("http://localhost:%d%s", myBusiPort, myBusiAPI)


var confPostgres = dtmcli.DBConf{
				Driver: "postgres",
				Host:   "localhost",
				Port:   5432,
				User:   "postgres",
				Password:"310584",
				Db:		"postgres",
			}
			
var confMysql = dtmcli.DBConf{
				Driver: "mysql",
				Host:   "localhost",
				Port:   3306,
				User:   "root",
				Password:"mysqlroot",
				Db:		"dtm_busi",
			}
			
func dbGetMy(conf *dtmcli.DBConf) *sql.DB {
	db, err := dtmimp.PooledDB(*conf)
	logger.FatalIfError(err)
	return db
}

func AdjustBalanceForType(db dtmcli.DB, dbtype string, uid int, amount int, result string) error {
	if strings.Contains(result, dtmcli.ResultFailure) {
		return dtmcli.ErrFailure
	}
	
	logger.Debugf("adjust balance uid: %s amount: %s", uid, amount)
	
	_, err := dtmimp.DBExec(dbtype, db, "update dtm_busi.user_account set balance = balance + ? where user_id = ?", amount, uid)

	return err
}

func AdjustTccBalanceForType(db dtmcli.DB, dbtype string, uid int, amount int, result string) error {
	affected, err := dtmimp.DBExec(dbtype, db, `update dtm_busi.user_account
		set trading_balance=trading_balance-?,
		balance=balance+? where user_id=?`, amount, amount, uid)
	if err == nil && affected == 0 {
		return fmt.Errorf("update user_account 0 rows")
	}
	return err
}

func AdjustTradingForType(db dtmcli.DB, dbtype string, uid int, amount int) error {
	affected, err := dtmimp.DBExec(dbtype, db, `update dtm_busi.user_account
		set trading_balance=trading_balance+?
		where user_id=? and trading_balance + ? + balance >= 0`, amount, uid, amount)
	if err == nil && affected == 0 {
		return fmt.Errorf("update error, maybe balance not enough")
	}
	return err
}

func MustBarrierFromGin(c *gin.Context) *dtmcli.BranchBarrier {
	ti, err := dtmcli.BarrierFromQuery(c.Request.URL.Query())
	logger.FatalIfError(err)
	return ti
}

type ReqHTTP struct {
	Amount         int    `json:"amount"`
	TransInResult  string `json:"trans_in_result"`
	TransOutResult string `json:"trans_out_Result"`
	Store          string `json:"store"`
}

func reqFrom(c *gin.Context) *ReqHTTP {
	v, ok := c.Get("trans_req")
	if !ok {
		req := ReqHTTP{}
		err := c.BindJSON(&req)
		logger.FatalIfError(err)
		c.Set("trans_req", &req)
		v = &req
	}
	return v.(*ReqHTTP)
}

// QsStartSvr quick start: start server
func MyStartSvr() {
	gin.SetMode(gin.ReleaseMode)
	app := gin.New()
	myAddRoute(app)
	log.Printf("example listening at %d", myBusiPort)
	go func() {
		_ = app.Run(fmt.Sprintf(":%d", myBusiPort))
	}()
	time.Sleep(500 * time.Millisecond)
}

var counter = 0

func myAddRoute(app *gin.Engine) {
	app.POST(myBusiAPI+"/MyTransIn", func(c *gin.Context) {
		log.Printf("MyTransIn")
		
		dtmimp.SetCurrentDBType("mysql")
		barrier := MustBarrierFromGin(c)
		barrier.CallWithDB(dbGetMy(&confMysql), func(tx *sql.Tx) error {
			return AdjustBalanceForType(tx, "mysql", 2, reqFrom(c).Amount, reqFrom(c).TransOutResult)
		})
		
		c.JSON(200, "OK")
		
		//AdjustBalanceForType(dbGetMy(&confMysql), "mysql", 2, reqFrom(c).Amount, reqFrom(c).TransInResult)		
		//c.JSON(409, "status conflict")
	})
	app.POST(myBusiAPI+"/MyTransOut", func(c *gin.Context) {
		log.Printf("MyTransOut")
		
		dtmimp.SetCurrentDBType("postgres")
		barrier := MustBarrierFromGin(c)
		barrier.CallWithDB(dbGetMy(&confPostgres), func(tx *sql.Tx) error {
			return AdjustBalanceForType(tx, "postgres", 1, -reqFrom(c).Amount, reqFrom(c).TransOutResult)
		})
		c.JSON(200, "OK")		
		//c.JSON(409, "status conflict")
	})
	app.GET(myBusiAPI+"/MyQueryPrepared", func(c *gin.Context) {
		logger.Debugf("%s MyQueryPrepared", c.Query("gid"))
		bb := MustBarrierFromGin(c)
		db := dbGetMy(&confPostgres)		
		err := bb.QueryPrepared(db)

		if err != nil {
			logger.Errorf("error query prepared : %s", err.Error())
		}
		
		c.JSON(200, "OK")		
	})
	app.POST(myBusiAPI+"/MyTransInCompensate", func(c *gin.Context) {
		log.Printf("MyTransInCompensate")
		
		dtmimp.SetCurrentDBType("mysql")
		barrier := MustBarrierFromGin(c)
		barrier.CallWithDB(dbGetMy(&confMysql), func(tx *sql.Tx) error {
			return AdjustBalanceForType(tx, "mysql", 2, -reqFrom(c).Amount, reqFrom(c).TransInResult)
		})
		
		c.JSON(200, "OK")
		//c.JSON(500, "server error")
	})
	app.POST(myBusiAPI+"/MyTransOutCompensate", func(c *gin.Context) {
		log.Printf("MyTransOutCompensate")
		
		dtmimp.SetCurrentDBType("postgres")
		barrier := MustBarrierFromGin(c)
		barrier.CallWithDB(dbGetMy(&confPostgres), func(tx *sql.Tx) error {
			return AdjustBalanceForType(tx, "postgres", 1, reqFrom(c).Amount, reqFrom(c).TransOutResult)
		})
		c.JSON(200, "OK")		
	})
	app.POST(myBusiAPI+"/MyTransInTry", func(c *gin.Context) {
		log.Printf("MyTransInTry")
		
		dtmimp.SetCurrentDBType("mysql")
		barrier := MustBarrierFromGin(c)
		
		err := barrier.CallWithDB(dbGetMy(&confMysql), func(tx *sql.Tx) error {
			return AdjustTradingForType(tx, "mysql", 2, reqFrom(c).Amount)
		})
		
		if err != nil {
			logger.Errorf("error MyTransInTry : %s", err.Error())
		}
		
		c.JSON(200, "OK")		
		//c.JSON(409, "conflict")				
		//c.JSON(500, "server error")
	})
	app.POST(myBusiAPI+"/MyTransOutTry", func(c *gin.Context) {
		log.Printf("MyTransOutTry")
		
		dtmimp.SetCurrentDBType("postgres")
		barrier := MustBarrierFromGin(c)
		
		err := barrier.CallWithDB(dbGetMy(&confPostgres), func(tx *sql.Tx) error {
			return AdjustTradingForType(tx, "postgres", 1, -reqFrom(c).Amount)
		})
		
		if err != nil {
			logger.Errorf("error MyTransOutTry : %s", err.Error())
		}
		
		c.JSON(200, "OK")		
	})
	app.POST(myBusiAPI+"/MyTransInConfirm", func(c *gin.Context) {
		log.Printf("MyTransInConfirm")	
		
		dtmimp.SetCurrentDBType("mysql")
		err := MustBarrierFromGin(c).CallWithDB(dbGetMy(&confMysql), func(tx *sql.Tx) error {
			return AdjustTccBalanceForType(tx, "mysql", 2, reqFrom(c).Amount, reqFrom(c).TransInResult)									
		})
		
		if err != nil {
			logger.Errorf("error MyTransInConfirm : %s", err.Error())
		}
		
		c.JSON(200, "OK")		
	})
	app.POST(myBusiAPI+"/MyTransOutConfirm", func(c *gin.Context) {
		log.Printf("MyTransOutConfirm")	

		dtmimp.SetCurrentDBType("postgres")
		err := MustBarrierFromGin(c).CallWithDB(dbGetMy(&confPostgres), func(tx *sql.Tx) error {
			return AdjustTccBalanceForType(tx, "postgres", 1, -reqFrom(c).Amount, reqFrom(c).TransOutResult)									
		})
		
		if err != nil {
			logger.Errorf("error MyTransOutConfirm : %s", err.Error())
		}

		c.JSON(200, "OK")				
		//c.JSON(500, "server error")						
	})
	app.POST(myBusiAPI+"/MyTransInCancel", func(c *gin.Context) {
		log.Printf("MyTransInCancel")	

		dtmimp.SetCurrentDBType("mysql")
		err := MustBarrierFromGin(c).CallWithDB(dbGetMy(&confMysql), func(tx *sql.Tx) error {
			return AdjustTradingForType(tx, "mysql", 2, -reqFrom(c).Amount)
		})
		
		if err != nil {
			logger.Errorf("error MyTransInCancel : %s", err.Error())
		}
		
		c.JSON(200, "OK")						
	})
	app.POST(myBusiAPI+"/MyTransOutCancel", func(c *gin.Context) {
		log.Printf("MyTransOutCancel")	
		
		dtmimp.SetCurrentDBType("postgres")
		err := MustBarrierFromGin(c).CallWithDB(dbGetMy(&confPostgres), func(tx *sql.Tx) error {
			return AdjustTradingForType(tx, "postgres", 1, reqFrom(c).Amount)
		})
		
		if err != nil {
			logger.Errorf("error MyTransOutCancel : %s", err.Error())
		}
		
		c.JSON(200, "OK")				
	})
	app.POST(myBusiAPI+"/MyTransInXa", func(c *gin.Context) {
		logger.Debugf("MyTransInXa")
				
		err := dtmcli.XaLocalTransaction(c.Request.URL.Query(), confMysql, func(db *sql.DB, xa *dtmcli.Xa) error {
			return AdjustBalanceForType(db, "mysql", 2, reqFrom(c).Amount, reqFrom(c).TransInResult)
		})
		
		if err != nil  {
			logger.Errorf("error MyTransInXa: %s", err.Error())
		}
		
		/*counter++;
		
		if counter == 1 {
			c.JSON(409, "conflict")
			return
		}*/
		
		c.JSON(200, "OK")
	})
	app.POST(myBusiAPI+"/MyTransOutXa", func(c *gin.Context) {
		logger.Debugf("MyTransOutXa")
		
		err := dtmcli.XaLocalTransaction(c.Request.URL.Query(), confPostgres, func(db *sql.DB, xa *dtmcli.Xa) error {
			return AdjustBalanceForType(db, "postgres", 1, -reqFrom(c).Amount, reqFrom(c).TransOutResult)
		})
		
		if err != nil  {
			logger.Errorf("error MyTransOutXa: %s", err.Error())
		}
		
		c.JSON(200, "OK")
	})
}

const dtmServer = "http://localhost:36789/api/dtmsvr"

func MsgTest() string {	
	req := &ReqHTTP{Amount: 50}
	msg := dtmcli.NewMsg(dtmServer, shortuuid.New()).
	Add(myBusi+"/MyTransIn", req)
		
	db := dbGetMy(&confPostgres)
	dtmimp.SetCurrentDBType("postgres")
	
	err := msg.DoAndSubmitDB(myBusi+"/MyQueryPrepared", db, func(tx *sql.Tx) error {		
		return AdjustBalanceForType(tx, "postgres", 1, -req.Amount, "SUCCESS")		
		//return dtmcli.ErrFailure
	})
	
	if err != nil {
		logger.Errorf("error msg test local: %s", err.Error())
	}
	
	return msg.Gid // id глобальной транзакции
}

func SagaTest() string {
	req := &ReqHTTP{Amount: 50}
	saga := dtmcli.NewSaga(dtmServer, shortuuid.New()).
		Add(myBusi+"/MyTransOut", myBusi+"/MyTransOutCompensate", req).
		Add(myBusi+"/MyTransIn", myBusi+"/MyTransInCompensate", req)
	logger.Debugf("saga busi trans submit")
	saga.TimeoutToFail = 200
	err := saga.Submit()
	
	if err != nil {
		logger.Errorf("error saga test: %s", err.Error())
	}

	return saga.Gid // id глобальной транзакции
}

func TccTest() string {
	gid := shortuuid.New()
	err := dtmcli.TccGlobalTransaction(dtmServer, gid, func(tcc *dtmcli.Tcc) (*resty.Response, error) {
		req := &ReqHTTP{Amount: 50}
		resp, err := tcc.CallBranch(req, myBusi+"/MyTransOutTry", myBusi+"/MyTransOutConfirm", myBusi+"/MyTransOutCancel")
		if err != nil {
			return resp, err
		}
		return tcc.CallBranch(req, myBusi+"/MyTransInTry", myBusi+"/MyTransInConfirm", myBusi+"/MyTransInCancel")
	})

	if err != nil {
		logger.Errorf("error tcc test: %s", err.Error())
	}

	return gid // id глобальной транзакции
}

func XaTest() string {
	dtmimp.SetCurrentDBType("postgres")
	gid := shortuuid.New()
	err := dtmcli.XaGlobalTransaction(dtmServer, gid, func(xa *dtmcli.Xa) (*resty.Response, error) {
		resp, err := xa.CallBranch(ReqHTTP{Amount: 50}, myBusi+"/MyTransOutXa")
		if err != nil {
			return resp, err
		}
		return xa.CallBranch(ReqHTTP{Amount: 50}, myBusi+"/MyTransInXa")
	})
	
	if err != nil  {
		logger.Errorf("error is: %s", err.Error())
	}
	
	return gid // id глобальной транзакции
}

func main() {
	logger.InitLog("debug")
	logger.Infof("starting test")

	log.Printf("starting api")
	MyStartSvr()
	log.Printf(TccTest())
	select {}
}
