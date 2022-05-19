//go:build filelog
// +build filelog

package wrt

import (
	"io"
	"log"

	param "github.com/GoGraph/dygparam"
)

func New() io.Writer {
	return param.FileWriter
}
func Start(f *log.Logger) error {
	return nil
}

func Stop() {}
