package main

import (
	"github.com/scryer-media/weaver/e2e/internal/weaver"
	"os"
)

func main() {
	weaver.Run(os.Args[1:], "weaver-e2e")
}
