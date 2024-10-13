package acornq

import (
	bpool "github.com/newacorn/simple-bytes-pool"
	"io"
	"os"
	"runtime"
	"strconv"
	"time"
)

func LogStack(hint string, message string, skip int, dst io.Writer, ideMode bool) {
	ps := make([]uintptr, 8)
	fs := runtime.CallersFrames(ps[:runtime.Callers(skip+1, ps)])
	const timestampPrefix = "T="
	now := time.Now()
	pb := bpool.Get(1024)
	buf := pb.B
	buf = append(buf, timestampPrefix...)
	if ideMode {
		buf = now.AppendFormat(buf, time.RFC3339)
	} else {
		buf = strconv.AppendInt(buf, now.Unix(), 10)
	}
	buf = append(buf, " L=error H=\""...)
	buf = append(buf, hint...)
	buf = append(buf, "\" "...)
	buf = append(buf, "M=\""...)
	buf = append(buf, message...)
	buf = append(buf, "\" S=["...)
	if ideMode {
		buf = append(buf, "\n"...)
	}
	start := false
	for {
		f, more := fs.Next()
		if !more {
			break
		}
		if start {
			if ideMode {
				buf = append(buf, '\n')
			} else {
				buf = append(buf, '|')
			}
		}
		buf = append(buf, f.Function...)
		buf = append(buf, ':')
		if ideMode {
			buf = append(buf, '\n')
		}
		buf = append(buf, f.File...)
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, int64(f.Line), 10)
		start = true
	}
	if ideMode {
		buf = append(buf, '\n')
	}
	buf = append(buf, "]\n"...)
	dst = os.Stdout
	//goland:noinspection GoUnhandledErrorResult
	dst.Write(buf)
	pb.RecycleToPool00()
}
