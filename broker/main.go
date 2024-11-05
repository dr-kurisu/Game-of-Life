package main

import (
	"uk.ac.bris.cs/gameoflife/gol"
)

func main() {
	go gol.Run()
	select {}
}
