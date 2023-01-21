//go:build filelog
// +build filelog

package wrt

import (
	"io"
	"log"

	param "github.com/GoGraph/syslog/param"
)

func New() io.Writer {
	return param.FileLogr.Writer()
}
func Start(f *log.Logger) error {
	return nil
}

func Stop() {}
