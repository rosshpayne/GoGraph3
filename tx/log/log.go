package log

import (
	"fmt"
	"log"
	"strings"
	//	"github.com/GoGraph/tx/db"
	//"github.com/GoGraph/tx/query"
)

type LogLvl int

const (
	Alert LogLvl = iota
	Debug
	NoLog
)

var (
	logr    *log.Logger
	logLvl  LogLvl // Alert, Warning, High
	errlogr func(string, error)
)

func SetLogger(lg *log.Logger, level LogLvl) {
	logr = lg
	logLvl = level
}

func SetErrLogger(el func(l string, e error)) {
	errlogr = el
}

func TestErrLogger() {
	err := fmt.Errorf("Error: TestErrLoggerErr")
	LogAlert("TestErrLogger 1")
	LogErr(err)
	LogAlert("TestErrLogger 2")
	LogErr(err)
	LogAlert("TestErrLogger 3")
	LogErr(err)
	LogAlert("TestErrLogger 4")
	LogErr(err)
	LogAlert("TestErrLogger 5")
	LogErr(err)
	LogAlert("TestErrLogger 6")
	LogErr(err)
}

func Err(e error) {
	LogErr(e)
}

func LogErr(e error) {

	if errlogr != nil {
		errlogr(logr.Prefix(), e)
	}

	var out strings.Builder

	out.WriteString("|error|")
	out.WriteString(e.Error())
	if logr == nil {
		fmt.Println(out.String())
	} else {
		logr.Print(out.String())
	}

}

func LogFatal(e error) {
	LogFail(e)
}
func LogFail(e error) {

	if errlogr != nil {
		errlogr(logr.Prefix(), e)
	}
	var out strings.Builder

	out.WriteString("|fatal|")
	out.WriteString(e.Error())
	if logr == nil {
		fmt.Println(out.String())
		panic(fmt.Errorf("Fatal error: %s", out.String()))
	} else {
		logr.Fatal(out.String())
	}

}

func LogDebug(s string) {

	if logLvl == Debug {
		var out strings.Builder

		out.WriteString("|info|")
		out.WriteString(s)
		if logr == nil {
			fmt.Println(out.String())
		} else {
			logr.Print(out.String())
		}
	}
}

func LogAlert(s string) {

	if logLvl <= Debug {
		var out strings.Builder
		out.WriteString("|alert|")
		out.WriteString(s)
		if logr == nil {
			fmt.Println(out.String())
		} else {
			logr.Print(out.String())
		}
	}
}
