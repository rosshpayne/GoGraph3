package log

var (
	logr    []log.Logger
	lgLvl   db.LogLvl // Alert, Warning, High
	errlogr func(string, error)
)

func SetLogger(lg log.Logger, level ...db.LogLvl) {

	logr = lg
	if len(level) == 0 {
		logLvl = db.Alert
	} else {
		logLvl = level[0]
	}
	query.SetLogger(lg, logLvl)
	//
	db.SetLogger(lg, logLvl)
}

func SetErrLogger(el func(l string, e error)) {
	errlogr = el
	query.SetErrLogr(el)
	//
	db.SetErrLogger(el)
}



func Err(e error) {
    LogErr(e)
}

func LogErr(e error) {
	log.LogErr(e)
	var out strings.Builder

	out.WriteString(":error:")
	out.WriteString(e.Error())

	logr.Print(out.String())
	errlogr(e)
}

func Fail(e error) {
    LogFail(e)
}
func LogFail(e error) {

	errlogr(e)
	var out strings.Builder

	out.WriteString(":error:")
	out.WriteString(e.Error())

	logr.Fatal(out.String())

}


func Debug(s string) {
    LogDebug(s)
}

func LogDebug(s string) {
	if logLvl == db.Debug {
		var out strings.Builder

		out.WriteString(":debug:")
		out.WriteString(s)
		logr.Print(out.String())
	}
}

func Alert(s string) {
    LogAlert(s)
}
func LogAlert(s string) {
	if logLvl <= db.Debug {
		var out strings.Buffer
		out.WriteString(":alert:")
		out.WriteString(s)
		logr.Print(out.String())
	}
}