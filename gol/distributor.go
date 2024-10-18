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

func workerQueue(cMatrix *countingMatrix, taskQueue <-chan util.Cell, wg *sync.WaitGroup) {
	for {
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
		wg.Done()
	}

}

func workerSegment(p Params, world [][]byte, startX, endX, startY, endY int, result [][]chan byte, wg *sync.WaitGroup) {
	defer wg.Done()
	for y := startY; y < endY; y++ {
		for x := startX; x < endX; x++ {
			counter := 0
			for _, d := range dir {
				if world[(y+d[0]+p.ImageHeight)%p.ImageHeight][(x+d[1]+p.ImageWidth)%p.ImageWidth] == byte(255) {
					counter++
				}
			}
			if counter == 3 {
				result[y][x] <- byte(255)
			} else if counter < 2 || counter > 3 {
				result[y][x] <- byte(0)
			} else {
				result[y][x] <- world[y][x]
			}
		}
	}

}

func evolutionSegment(p Params, world [][]byte) [][]byte {
	result := make([][]chan byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		result[i] = make([]chan byte, p.ImageWidth)
		for j := 0; j < p.ImageWidth; j++ {
			result[i][j] = make(chan byte, 1)
		}
	}

	y_interval := p.ImageHeight / p.ImageWidth
	wg := sync.WaitGroup{}
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		startY := i * y_interval
		endY := (i + 1) * y_interval
		if i == p.Threads-1 {
			endY = p.ImageHeight
		}
		go workerSegment(p, world, 0, p.ImageWidth, startY, endY, result, &wg)
	}

	matrix := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		matrix[i] = make([]byte, p.ImageWidth)
	}

	wg.Wait()
	for i := 0; i < p.ImageHeight; i++ {
		for j := 0; j < p.ImageWidth; j++ {
			matrix[i][j] = <-result[i][j]
		}
	}
	return matrix
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

	go func() { // like a producer
		for _, cell := range aliveCells {
			taskQueue <- cell
		}
	}()

	for i := 0; i < p.Threads; i++ {
		go workerQueue(&cMatrix, taskQueue, &wg)
	}
	wg.Wait()
	result := make([]util.Cell, 0)
	cellMap := make(map[util.Cell]bool)
	for _, cell := range aliveCells {
		cellMap[cell] = true
	}
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
	//var aliveCells []util.Cell
	//aliveCells = checker(world)
	//for i := 0; i < p.Turns; i++ {
	//	aliveCells = evolution(p, aliveCells)
	//}
	for i := 0; i < p.Turns; i++ {
		world = evolutionSegment(p, world)
	}
	aliveCells := checker(world)

	c.events <- FinalTurnComplete{p.Turns, aliveCells}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
