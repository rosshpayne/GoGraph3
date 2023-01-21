package syslog

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	mparam "github.com/GoGraph/dygparam"
	"github.com/GoGraph/syslog/internal/wrt"
	"github.com/GoGraph/syslog/param"
)

const (
	logrFlags = log.LstdFlags | log.Lshortfile

	logDir  = "/GoGraph/"
	logName = "GoGraph"
	idFile  = "log.id"
)

type logEntry struct {
	logr  *log.Logger
	ready chan struct{}
}

// global logger - accessible from any routine
var (
	//logr *log.Logger
	iow io.Writer
	//
	logrMap map[string]*logEntry
	logLck  sync.Mutex
)

func init() {

	logrMap = make(map[string]*logEntry)

}

// Start called from main after runid is created.
func Start() error {
	prefix := "main"

	// create a logger to be used to support the main logger (osfile or CWLogs)
	if param.FileLogr == nil {
		fileLogr := log.New(NewBaseErrFile(), prefix, logrFlags)
		fileLogr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
		param.FileLogr = fileLogr
		e := &logEntry{logr: fileLogr, ready: make(chan struct{})}
		close(e.ready)
		logrMap[prefix] = e
	}

	// assign either a file io.Writer or CloudWatchLogs io.Writer - determined by build tags (TODO: consider determining via parameter)
	iow = wrt.New()
	return wrt.Start(param.FileLogr)

}

func Stop() {
	Log("syslog", "Shutting down syslog service")
	wrt.Stop()
}

func NewLogr(prefix string) *log.Logger {
	return getLogr(prefix)
}

func newLogr(prefix string) *log.Logger {

	var s strings.Builder
	s.WriteString(prefix)
	s.WriteByte('|')

	if iow == nil {
		panic(fmt.Errorf("execute syslog.Start() before using syslog"))
	}

	logr := log.New(iow, s.String(), logrFlags)
	logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	logr.Printf("create new logr for prefix: %q", prefix)

	return logr
}

func getLogr(prefix string) *log.Logger {

	var ok bool

	// get a logger from the map for the particular prefix -
	// implement using non-blocking shared cache (map)
	logLck.Lock()
	e, ok := logrMap[prefix]
	if !ok {
		// create new logger entry and save to map
		e = &logEntry{logr: newLogr(prefix), ready: make(chan struct{})}
		logrMap[prefix] = e
		logLck.Unlock()
		close(e.ready)
	} else {

		logLck.Unlock()
		<-e.ready
	}
	return e.logr

}

func LogDebug(prefix string, s string) {
	Log(prefix, s)
}

// Log is the main function for logging text to the underlying storage system either an os file or AWS Cloudwatch logs. TODO: implement Google equivalent
func Log(prefix string, s string) {

	prefix = strings.TrimRight(prefix, " :")
	// check if prefix is on the must log services. These will be logged even if parameter logging is false.
	var logit bool
	if !mparam.DebugOn {
		for _, s := range mparam.LogServices {
			//         HasPrefix(dest (logid),source (parameter) - dest has prefix s
			if strings.HasPrefix(prefix, s) {
				logit = true
				break
			}
		}
	}
	// abandon logging if both of these conditions are false
	if !logit && !mparam.DebugOn {
		return
	}
	//
	// previously a single logger was used - this required a full lock to support multiple prefix values as it was noted a
	// prefix could not be changed without the potential for corruption of the logger output.
	// Now, individual logger's are created for each prefix. This enables the use of reader locks rather than full read/write locks
	// (write (full) lock only when a new prefix is added) consequently multiple writers can run concurrently which requires the
	// associated io.Writer to serialised access to its underlying resource.
	//
	// note: this design is better then a "service" to serialises access as a service will serialise all access whereas the current design for the logger
	// will only serialises access (full write lock) for a new Prefix value - all other access is based on a read lock which enables concurrent access.
	//
	// if logrMap == nil {
	// 	fileLogr := log.New(NewBaseErrFile(), "main", logrFlags)
	// 	fileLogr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	// 	param.FileLogr = fileLogr
	// 	fileLogr.Printf(s)
	// 	return
	// }
	// get a logger from the map for the particular prefix

	logr := getLogr(prefix)
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.
	var out strings.Builder
	out.WriteString("|info|")
	out.WriteString(s)
	logr.Print(out.String())
	fmt.Println("leave Log(")
}

func LogAlert(prefix string, s string) {

	logr := getLogr(prefix)
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.
	var out strings.Builder
	out.WriteString("|alert|")
	out.WriteString(s)
	logr.Print(out.String())

}

func LogErr(prefix string, e error) {

	logr := getLogr(prefix)
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.
	var out strings.Builder
	out.WriteString("|error|")
	out.WriteString(e.Error())

	logr.Print(out.String())

}

func LogFail(prefix string, s error) {

	logr := getLogr(prefix)
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.
	var out strings.Builder
	out.WriteString("|fatal|")
	out.WriteString(s.Error())

	logr.Fatal(out.String())
}

// func Logf(prefix string, format string, v ...interface{}) {

// 	var ok bool
// 	// get a logger from the map for the particular prefix
// 	logWRm.RLock()
// 	logr, ok = logrMap[prefix]
// 	logWRm.RUnlock()

// 	if !ok {
// 		// create new logger and save to map
// 		logr = newLogr(prefix)
// 		logWRm.Lock()
// 		logrMap[prefix] = logr
// 		logWRm.Unlock()
// 	}

// 	if len(panic) != 0 && panic[0] {
// 		logr.Panic(s)
// 		logWRm.RUnlock()
// 		return
// 	}
// 	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
// 	// which is either a os.File or Cloudwatch Logs.
// 	logr.Print(s)

// 	logr.SetPrefix(prefix)
// 	logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
// 	fmt.Println(format)
// 	logr.Printf(format, v...)

// }

// func LogF(prefix string, s string) {
// 	// log it
// 	prefixMutex.Lock()
// 	logr.SetPrefix(prefix)
// 	logr.Print(s)
// 	prefixMutex.Unlock()
// }

// NewBaseErrFile creates a log file based on name GoGraph.[a..z].log. Current  a..z value stored in log.id file in directory LOGDIR (envion variable)
func NewBaseErrFile() io.Writer {
	//
	// open log id file (contains: a..z) used to generate log files with naming convention <logDIr><logName>.<a..z>.log
	//
	if s := os.Getenv("LOGDIR"); len(s) == 0 {
		log.Fatal(fmt.Errorf("LOGDIR not defined. Define LOGDIR as the full path to the log directory"))
	}
	idf, err := os.OpenFile(os.Getenv("LOGDIR")+logDir+idFile, os.O_RDWR|os.O_CREATE, 0744)
	if err != nil {
		log.Fatal(err)
	}
	//
	// read log id into postfix and update and save back to file
	//
	var n int
	postfix := make([]uint8, 1, 1)
	n, err = idf.Read(postfix)
	if err != nil && err != io.EOF {
		log.Fatalf("log: error in reading log.id, %s", err.Error())
	}
	if n == 0 {
		postfix[0] = 'a'
	} else {
		if postfix[0] == 'z' {
			postfix[0] = 'a'
		} else {
			postfix[0] += 1
		}
	}
	// reset file to beginning and save postfix
	idf.Seek(0, 0)
	_, err = idf.Write(postfix)
	if err != nil {
		log.Fatalf("log: error in writing to id file, %s", err.Error())
	}
	err = idf.Close()
	if err != nil {
		panic(err)
	}
	//
	var s strings.Builder
	s.WriteString(os.Getenv("LOGDIR"))
	s.WriteString(logDir)
	s.WriteString(logName)
	s.WriteByte('.')
	s.WriteByte(postfix[0])
	s.WriteString(".log")

	param.LogFile = s.String()

	logf, err := os.OpenFile(s.String(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	param.FileWriter = logf
	return logf
}
