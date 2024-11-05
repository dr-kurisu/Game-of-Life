package main

import (
	"uk.ac.bris.cs/gameoflife/gol"
)

// main is the function called when starting Game of Life with 'go run .'
func main() {
	go gol.Run()
	select {}
}
