package gol

import (
	"fmt"
	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

func counting(world [][]byte, x, y int) int {
	dir := [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}
	counter := 0
	for _, d := range dir {
		if world[x+d[0]][y+d[1]] == byte(255) {
			counter++
		}
	}
	return counter
}

func evolution(world [][]byte) [][]byte {
	new_world := make([][]byte, len(world))
	for i := 0; i < len(world); i++ {
		new_world[i] = make([]byte, len(world[0]))
	}
	for x, _ := range world {
		for y, _ := range world[x] {
			nums := counting(world, x, y)
			switch {
			case nums == 3:
				new_world[x][y] = byte(255)
			case nums > 3 || nums < 2:
				new_world[x][y] = byte(0)
			default:
				new_world[x][y] = world[x][y]
			}

		}
	}
	return new_world
}

func checker(world [][]byte) []util.Cell {
	result := make([]util.Cell, 0)
	for x, _ := range world {
		for y, _ := range world[x] {
			if world[x][y] == byte(255) {
				result = append(result, util.Cell{x, y})
			}
		}
	}
	return result
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
	fmt.Println(filename)
	c.ioFilename <- filename

	for index := 0; index < p.ImageHeight*p.ImageWidth; index++ {
		world[index/p.ImageWidth][index%p.ImageWidth] = <-c.ioInput
	}

	// run evolution until end of turns
	for i := 0; i < p.Turns; i++ {
		world = evolution(world)
	}

	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{p.Turns, checker(world)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
