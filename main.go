package main

import (
	"log"

	"github.com/Percona-Lab/mysql_random_data_load/cmd"
	"github.com/alecthomas/kong"
)

var (
	cli struct {
		Run cmd.RunCmd `cmd:"run" help:"Starts the insert process"`
	}

	Version   = "0.0.0."
	Commit    = "<sha1>"
	Branch    = "branch-name"
	Build     = "2017-01-01"
	GoVersion = "1.9.2"
)

const (
	defaultMySQLConfigSection = "client"
)

func main() {
	ctx := kong.Parse(&cli,
		kong.Name("MySQL random data loader"),
		kong.Description("Load random data into a MySQL table"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: false,
			Summary: true,
			Tree:    true,
		}),
	)
	switch ctx.Command() {
	case "run":
		if err := ctx.Run(); err != nil {
			log.Fatalf(err.Error())
		}
	default:
		log.Fatalf("Unknown command")
	}
}
