package main

import (
	"flag"
	"uk.ac.bris.cs/gameoflife/gol"
)

// main is the function called when starting Game of Life with 'go run .'
func main() {
	port := flag.String("port", ":8030", "Port number for the server to listen on")
	flag.Parse()
	go gol.Run(*port)
	select {}
}
