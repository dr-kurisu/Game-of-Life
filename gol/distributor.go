package gol

import (
	"fmt"
	"sync"
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

type countingMatrix struct {
	mutex  sync.Mutex
	matrix [][]int8
	height int
	width  int
}

var dir = [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}

func worker(cMatrix *countingMatrix, taskQueue <-chan util.Cell, wg *sync.WaitGroup) {
	defer wg.Done()
	task := <-taskQueue
	height := cMatrix.height
	width := cMatrix.width

	for _, d := range dir {
		realY := (task.Y + d[0] + height) % height
		realX := (task.X + d[1] + width) % width
		cMatrix.mutex.Lock()
		cMatrix.matrix[realY][realX] += 1
		cMatrix.mutex.Unlock()
	}
}

func evolution(p Params, aliveCells []util.Cell) []util.Cell {
	matrix := make([][]int8, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		matrix[i] = make([]int8, p.ImageWidth)
	}
	cMatrix := countingMatrix{}
	cMatrix.matrix = matrix
	cMatrix.height = p.ImageHeight
	cMatrix.width = p.ImageWidth

	numAlive := len(aliveCells)
	taskQueue := make(chan util.Cell, numAlive)
	wg := sync.WaitGroup{}
	wg.Add(numAlive)

	go func() {
		for _, cell := range aliveCells {
			taskQueue <- cell
		}
	}()

	for i := 0; i < p.Threads; i++ {
		go worker(&cMatrix, taskQueue, &wg)
	}
	wg.Wait()
	result := make([]util.Cell, 0)
	cellMap := make(map[util.Cell]bool)
	for _, cell := range aliveCells {
		cellMap[cell] = true
	}
	fmt.Println(result)
	for y, _ := range cMatrix.matrix {
		for x, _ := range cMatrix.matrix[y] {
			if cMatrix.matrix[y][x] == 3 {
				result = append(result, util.Cell{X: x, Y: y})
			} else if cMatrix.matrix[y][x] >= 2 && cMatrix.matrix[y][x] <= 3 && cellMap[util.Cell{X: x, Y: y}] {
				result = append(result, util.Cell{X: x, Y: y})
			}
		}
	}
	return result
}

func checker(world [][]byte) []util.Cell {
	result := make([]util.Cell, 0)
	for y, _ := range world {
		for x, _ := range world[y] {
			if world[y][x] == byte(255) {
				result = append(result, util.Cell{X: x, Y: y})
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

	c.ioCommand <- ioInput
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioFilename <- filename

	for index := 0; index < p.ImageHeight*p.ImageWidth; index++ {
		world[index/p.ImageWidth][index%p.ImageWidth] = <-c.ioInput
	}

	// run evolution until end of turns
	var aliveCells []util.Cell
	aliveCells = checker(world)
	for i := 0; i < p.Turns; i++ {
		aliveCells = evolution(p, aliveCells)
	}

	c.events <- FinalTurnComplete{p.Turns, aliveCells}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
