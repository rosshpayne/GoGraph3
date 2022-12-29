package syslog

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/syslog/internal/wrt"
)

const (
	logrFlags = log.LstdFlags | log.Lshortfile

	logDir  = "/GoGraph/"
	logName = "GoGraph"
	idFile  = "log.id"
)

// global logger - accessible from any routine
var (
	//logr *log.Logger
	iow io.Writer
	//
	logrMap map[string]*log.Logger
	logWRm  sync.RWMutex

	logrAlertMap map[string]*log.Logger
	alertWRm     sync.RWMutex

	logrErrMap map[string]*log.Logger
	errWRm     sync.RWMutex
)

func init() {

	logrMap = make(map[string]*log.Logger)
	logrAlertMap = make(map[string]*log.Logger)
	logrErrMap = make(map[string]*log.Logger)

}

// Start called from main after runid is created.
func Start() error {
	logrMap = make(map[string]*log.Logger)

	// create a logger to be used to support the main logger (osfile or CWLogs)
	if param.FileLogr == nil {
		fileLogr := log.New(NewBaseErrFile(), "main", logrFlags)
		fileLogr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
		param.FileLogr = fileLogr
	}

	// assign either a file io.Writer or CloudWatchLogs io.Writer - determined by build tags (TODO: consider determining via parameter)
	iow = wrt.New()
	return wrt.Start(param.FileLogr)

}

func Stop() {
	Log("syslog", "Shutting down syslog service")
	wrt.Stop()
}

func newLogr(prefix string, logType string) *log.Logger {

	fmt.Printf("*** create new logr for prefix: [%s]\n", prefix)

	var s strings.Builder
	s.WriteString(prefix)
	s.WriteByte(':')
	s.WriteString(logType)
	s.WriteByte(':')

	if iow == nil {
		panic(fmt.Errorf("execute syslog.Start() before using syslog"))
	}

	logr := log.New(iow, s.String(), logrFlags)
	logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	return logr
}

// Log is the main function for logging text to the underlying storage system either an os file or AWS Cloudwatch logs. TODO: implement Google equivalent
func Log(prefix string, s string, panic ...bool) {

	prefix = strings.TrimRight(prefix, " :")
	// check if prefix is on the must log services. These will be logged even if parameter logging is false.
	var logit bool
	if !param.DebugOn {
		for _, s := range param.LogServices {
			//         HasPrefix(dest (logid),source (parameter) - dest has prefix s
			if strings.HasPrefix(prefix, s) {
				logit = true
				break
			}
		}
	}
	// abandon logging if both of these conditions are false
	if !logit && !param.DebugOn {
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
	var ok bool
	// get a logger from the map for the particular prefix

	logWRm.RLock()
	logr, ok := logrMap[prefix]
	logWRm.RUnlock()

	if !ok {
		// create new logger and save to map
		logr = newLogr(prefix, "info")
		if logr == nil {
			fmt.Println("logr is nil")
		}
		logWRm.Lock()
		logrMap[prefix] = logr
		logWRm.Unlock()
	}
	if len(panic) > 0 && panic[0] {
		logr.Panic(s)
		return
	}
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.

	logr.Print(s)
}

func LogAlert(prefix string, s string, panic ...bool) {

	var ok bool

	// get a logger from the map for the particular prefix
	alertWRm.RLock()
	logr, ok := logrAlertMap[prefix]
	alertWRm.RUnlock()
	if !ok {
		fmt.Println("create new alert logr for prefix: ", prefix)
		// create new logger and save to map
		logr = newLogr(prefix, "alert")
		alertWRm.Lock()
		logrAlertMap[prefix] = logr
		alertWRm.Unlock()
	}
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.
	logr.Print(s)

}
func LogErr(prefix string, s string, panic ...bool) {

	var ok bool

	// get a logger from the map for the particular prefix
	errWRm.RLock()
	logr, ok := logrErrMap[prefix]
	errWRm.RUnlock()
	if !ok {
		fmt.Println("create new critical logr for prefix: ", prefix)
		// create new logger and save to map
		logr = newLogr(prefix, "critical")
		errWRm.Lock()
		logrErrMap[prefix] = logr
		errWRm.Unlock()
	}
	// note: as a result of read mutex loggers can be called concurrently. All loggers use the same io.Writer
	// which is either a os.File or Cloudwatch Logs.
	logr.Print(s)

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
