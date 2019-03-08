package main

import (
	"os"

	"github.com/kckecheng/unitybeat/cmd"

	_ "github.com/kckecheng/unitybeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
