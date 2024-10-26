package gol

import (
	"fmt"
	"log"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/communication"
	"uk.ac.bris.cs/gameoflife/util"
)

var GolWorld [][]byte

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

func checker(world [][]byte) []util.Cell {
	result := make([]util.Cell, 0)
	for x, _ := range world {
		for y, _ := range world[x] {
			if world[x][y] == byte(255) {
				result = append(result, util.Cell{y, x})
			}
		}
	}
	return result
}
func comWithServer(client *rpc.Client, world [][]byte, p Params) {
	send := communication.ReceiveFromServer{P: p, CurrentWorld: world}
	res := new(communication.SendToClient)
	client.Call("server.connectionHandler", send, res)
	GolWorld = res.World
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	world := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		world[i] = make([]byte, p.ImageWidth)
	}
	turn := 0
	c.events <- StateChange{turn, Executing}
	// TODO: Execute all turns of the Game of Life.
	c.ioCommand <- ioInput
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioFilename <- filename
	for index := 0; index < p.ImageHeight*p.ImageWidth; index++ {
		world[index/p.ImageWidth][index%p.ImageWidth] = <-c.ioInput
	}
	// initialise the client
	server := "" // Ip to be confirmed
	client, err := rpc.Dial("tcp", server)
	if err != nil { // error happens
		log.Fatal("dialing:", err)
	}
	defer client.Close()
	comWithServer(client, world, p)
	world = GolWorld
	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{p.Turns, checker(world)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
