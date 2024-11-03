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

var dir = [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}

func worker(partWorld [][]byte, partResult [][]byte, wg *sync.WaitGroup) {
	defer wg.Done()
	interval := len(partResult)
	width := len(partResult[0])
	for y := 0; y < interval; y++ {
		for x := 0; x < width; x++ {
			for _, d := range dir {
				if partWorld[y+d[0]+1][(x+d[1]+width)%width] == byte(255) {
					partResult[y][x] += 1
				}
			}
		}
	}
	for y := 0; y < interval; y++ {
		for x := 0; x < width; x++ {
			if partResult[y][x] == byte(3) || (partResult[y][x] == byte(2) && partWorld[y+1][x] == byte(255)) {
				partResult[y][x] = byte(255)
			}
		}
	}
}

func centralDistributor(world [][]byte, p Params) [][]byte {
	yInterval := p.ImageHeight / p.Threads
	result := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		result[i] = make([]byte, p.ImageWidth)
	}
	// wg used to control each thread
	wg := sync.WaitGroup{}
	wg.Add(p.Threads)

	for i := 0; i < p.Threads; i++ {
		startY := i * yInterval
		endY := (i + 1) * yInterval
		// deal with the last thread
		if i == p.Threads-1 {
			endY = p.ImageHeight
		}
		partResult := result[startY:endY]
		partWorld := make([][]byte, 0)
		// deal with start and medium
		for offset := 0; offset < endY-startY+2; offset++ {
			partWorld = append(partWorld, world[(offset+startY-1+p.ImageHeight)%p.ImageHeight])
		}
		go worker(partWorld, partResult, &wg)
	}
	wg.Wait()
	return result
}

func checker(world [][]byte, p Params) []util.Cell {
	result := make([]util.Cell, 0)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	y_interval := p.ImageHeight / p.Threads
	ys := 0
	ye := y_interval
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		if i == p.Threads-1 {
			ye = p.ImageHeight
		}
		go func(ys, ye int) {
			defer wg.Done()
			localResult := make([]util.Cell, 0)
			for y := ys; y < ye; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if world[y][x] == byte(255) {
						localResult = append(localResult, util.Cell{X: x, Y: y})
					}
				}
			}
			mutex.Lock()
			result = append(result, localResult...)
			mutex.Unlock()
		}(ys, ye)
		ys += y_interval
		ye += y_interval
	}
	wg.Wait()
	return result
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels, keyPresses <-chan rune) {
	world := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		world[i] = make([]byte, p.ImageWidth)
	}

	turn := 0

	c.ioCommand <- ioInput
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioFilename <- filename

	for index := 0; index < p.ImageHeight*p.ImageWidth; index++ {
		world[index/p.ImageWidth][index%p.ImageWidth] = <-c.ioInput
	}
	aliveCells := checker(world, p)
	c.events <- CellsFlipped{0, aliveCells}
	c.events <- StateChange{turn, Executing}

	// run evolution until end of turns
	for i := 0; i < p.Turns; i++ {
		world = centralDistributor(world, p)
	}

	c.events <- FinalTurnComplete{p.Turns, checker(world, p)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
