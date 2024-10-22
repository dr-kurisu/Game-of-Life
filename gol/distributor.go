package gol

import (
	"fmt"
	"sync"
	"time"
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

func makeCountMat(world [][]byte, p Params) [][]int8 {
	y_interval := p.ImageHeight / p.Threads

	cMats := make([]chan [][]int8, p.Threads)
	for i := range cMats {
		cMats[i] = make(chan [][]int8, 1)
	}
	var wg sync.WaitGroup
	wg.Add(p.Threads)

	for i := 0; i < p.Threads; i++ {
		startY := i * y_interval
		endY := startY + y_interval
		if i == p.Threads-1 {
			endY = p.ImageHeight
		}
		i := i // intermediate var
		go func() {
			defer wg.Done()
			countWorker(startY, endY, world, cMats[i], p)
		}()
	}
	wg.Wait()
	result := make([][]int8, p.ImageHeight)
	for i := range result {
		result[i] = make([]int8, p.ImageHeight)
	}

	// Combine Counting Matrix
	for i := 0; i < p.Threads; i++ {
		startY := i * y_interval
		cMat := <-cMats[i]
		for y, _ := range cMat {
			for x, _ := range cMat[y] {
				result[(y-1+startY+p.ImageHeight)%p.ImageHeight][x] += cMat[y][x]
			}
		}
	}
	return result
}

func countWorker(startY, endY int, world [][]byte, cMatChan chan [][]int8, p Params) {
	// function to make counting matrix
	cMat := make([][]int8, endY-startY+2) // more rows in first line and last line
	for j := 0; j < endY-startY+2; j++ {
		cMat[j] = make([]int8, p.ImageWidth)
	}
	for y := startY; y < endY; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == byte(255) {
				for _, d := range dir {
					cMat[y-startY+d[0]+1][(x+d[1]+p.ImageWidth)%p.ImageWidth] += 1
				}
			}
		}
	}
	cMatChan <- cMat
}

func evolution(cMat [][]int8, world [][]byte, p Params) [][]byte {
	// Given a counting matrix return a list of alive cells
	y_interval := p.ImageHeight / p.Threads

	aliveCells := make([]util.Cell, 0)

	wg := sync.WaitGroup{}

	wg.Add(p.Threads)

	var mutex sync.Mutex

	for i := 0; i < p.Threads; i++ {
		startY := i * y_interval
		endY := (i + 1) * y_interval
		if i == p.Threads-1 {
			endY = p.ImageHeight
		}
		go func() {
			defer wg.Done()
			localAliveCells := []util.Cell{}
			for y := startY; y < endY; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if cMat[y][x] == 3 || cMat[y][x] == 2 && world[y][x] == byte(255) {
						localAliveCells = append(localAliveCells, util.Cell{X: x, Y: y})
					}
				}
			}
			mutex.Lock()
			aliveCells = append(aliveCells, localAliveCells...)
			mutex.Unlock()
		}()
	}
	wg.Wait()
	result := make([][]byte, p.ImageHeight)
	for i := range result {
		result[i] = make([]byte, p.ImageWidth)
	}
	for _, cell := range aliveCells {
		result[cell.Y][cell.X] = byte(255)
	}

	return result
}

func checker(world [][]byte, p Params) []util.Cell {
	result := make([]util.Cell, 0)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	averagetest := p.ImageHeight / p.Threads
	ys := 0
	ye := averagetest
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
		ys += averagetest
		ye += averagetest
	}
	wg.Wait()
	return result
}

func aliveCounter(world [][]byte, p Params) int {
	result := 0
	var mutex sync.Mutex
	var wg sync.WaitGroup
	averagetest := p.ImageHeight / p.Threads
	ys := 0
	ye := averagetest
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		if i == p.Threads-1 {
			ye = p.ImageHeight
		}
		go func(ys, ye int) {
			defer wg.Done()
			localResult := 0
			for y := ys; y < ye; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if world[y][x] == byte(255) {
						localResult += 1
					}
				}
			}
			mutex.Lock()
			result += localResult
			mutex.Unlock()
		}(ys, ye)
		ys += averagetest
		ye += averagetest
	}
	wg.Wait()
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

	ticker := time.NewTicker(2 * time.Second)

	// run evolution until end of turns
	for i := 0; i < p.Turns; i++ {
		cMat := makeCountMat(world, p)
		world = evolution(cMat, world, p)
		select {
		case <-ticker.C:
			nums := aliveCounter(world, p)
			c.events <- AliveCellsCount{i + 1, nums}
		default:
		}
	}

	c.events <- FinalTurnComplete{p.Turns, checker(world, p)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
