package acornq

import (
	"github.com/olekukonko/tablewriter"
	"os"
	"strconv"
)

const logo = `_______                          _______ 
___    |___________________________  __ \
__  /| |  ___/  __ \_  ___/_  __ \  / / /
_  ___ / /__ / /_/ /  /   _  / / / /_/ / 
/_/  |_\___/ \____//_/    /_/ /_/\___\_\ 
                                         `

func outputInfo(s *Server) {
	//goland:noinspection GoUnhandledErrorResult
	os.Stdout.Write([]byte(logo + "\n"))
	tb := tablewriter.NewWriter(os.Stdout)
	headers := []string{"PID", "WorkerCount", "PollInterval"}
	tb.SetHeader(headers)
	tb.SetColumnAlignment([]int{tablewriter.ALIGN_RIGHT, tablewriter.ALIGN_RIGHT, tablewriter.ALIGN_RIGHT})
	tb.SetHeaderColor(
		tablewriter.Colors{tablewriter.FgGreenColor},
		tablewriter.Colors{tablewriter.FgGreenColor},
		tablewriter.Colors{tablewriter.FgGreenColor})
	tb.SetColumnColor(tablewriter.Colors{tablewriter.Bold, tablewriter.FgHiBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgHiBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})
	tb.Append([]string{strconv.Itoa(os.Getpid()), strconv.Itoa(s.concurrency), s.taskPeekInterval.String()})
	tb.Render()
}
