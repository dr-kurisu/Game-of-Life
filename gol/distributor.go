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

type controlChannels struct {
	pause  chan bool
	quit   chan bool
	save   chan bool
	ticker *time.Ticker
}

type Result struct {
	result [][]byte
	up     []byte
	down   []byte
}

var dir = [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}

func singleThreadWorker(world, result [][]byte) {
	height := len(world)
	width := len(world[0])
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[y][x] == byte(255) {
				for _, d := range dir {
					result[(y+d[0]+height)%height][(x+d[1]+width)%width] += 1
				}
			}
		}
	}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if result[y][x] == byte(3) || (result[y][x] == byte(2) && world[y][x] == byte(255)) {
				result[y][x] = byte(255)
			}
		}
	}
}

func multiThreadWorker(partWorld [][]byte, result Result, startY, endY int, wg *sync.WaitGroup) {
	defer wg.Done()
	width := len(partWorld[0])
	height := len(partWorld)
	// deal with the first line
	for x := 0; x < width; x++ {
		if partWorld[0][x] == byte(255) {
			for _, d := range dir {
				if d[0] == -1 {
					result.up[(x+d[1]+width)%width] += 1
				} else {
					result.result[d[0]][(x+d[1]+width)%width] += 1
				}
			}
		}
	}
	// deal with the last line
	for x := 0; x < width; x++ {
		if partWorld[height-1][x] == byte(255) {
			for _, d := range dir {
				if d[0] == 1 {
					result.down[(x+d[1]+width)%width] += 1
				} else {
					result.result[height-1+d[0]][(x+d[1]+width)%width] += 1
				}
			}
		}
	}
	// deal with remaining
	for y := 1; y < height-1; y++ {
		for x := 0; x < width; x++ {
			if partWorld[y][x] == byte(255) {
				for _, d := range dir {
					result.result[y+d[0]][(x+d[1]+width)%width] += 1
				}
			}
		}
	}
}

func keyBoardDetection(keyPresses <-chan rune, controlChan controlChannels) {
	for {
		key := <-keyPresses
		switch key {
		case 's':
			controlChan.save <- true
		case 'q':
			controlChan.quit <- true
		case 'p':
			controlChan.pause <- true
		default:
			panic("Wrong input from keyboard")
		}
	}
}

func cellFlipHandling(world [][]byte, result [][]byte, p Params, c distributorChannels, turn int) {
	var wg sync.WaitGroup
	yInterval := p.ImageHeight / p.Threads
	ys := 0
	ye := yInterval
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		if i == p.Threads-1 {
			ye = p.ImageHeight
		}
		go func(ys, ye int) {
			defer wg.Done()
			for y := ys; y < ye; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if world[y][x] != byte(255) && result[y][x] == byte(255) || world[y][x] == byte(255) && result[y][x] != byte(255) {
						c.events <- CellFlipped{CompletedTurns: turn, Cell: util.Cell{X: x, Y: y}}
					}
				}
			}
		}(ys, ye)
		ys += yInterval
		ye += yInterval
	}
	wg.Wait()
}

func centralDistributor(world [][]byte, p Params, c distributorChannels, turn int) [][]byte {
	//yInterval := p.ImageHeight / p.Threads
	result := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		result[i] = make([]byte, p.ImageWidth)
	}

	//if p.Threads == 1 || p.ImageHeight/p.Threads < 4 {
	if p.Threads > 0 {
		singleThreadWorker(world, result)
	} else {
		interval := p.ImageHeight / p.Threads
		results := make([]Result, p.Threads)
		wg := sync.WaitGroup{}
		wg.Add(p.Threads)
		for i := 0; i < p.Threads; i++ {
			startY := interval * i
			endY := startY + interval
			if i == p.Threads-1 {
				endY = p.ImageHeight
			}
			// create efficient pointer for result
			results[i] = Result{
				result: result[startY:endY],
				up:     make([]byte, p.ImageWidth),
				down:   make([]byte, p.ImageWidth),
			}
			go multiThreadWorker(world[startY:endY], results[i], startY, endY, &wg)
		}
		wg.Wait()
		// combine results
		//for i, r := range results {
		//startY := interval * i
		//endY := startY + interval
		//if i == len(result)-1 {
		//	endY = p.ImageHeight
		//}
		//for x := 0; x < p.ImageWidth; x++ {
		//	result[(startY-1+p.ImageHeight)%p.ImageHeight][x] += r.up[x]
		//	result[(endY+p.ImageHeight)%p.ImageHeight][x] += r.down[x]
		//}
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				if result[y][x] == byte(3) || (result[y][x] == byte(2) && world[y][x] == byte(255)) {
					result[y][x] = byte(255)
				}
			}
		}
	}
	return result
}

func output(world [][]byte, c *distributorChannels, p Params, turn int) string {
	c.ioCommand <- ioOutput
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioFilename <- filename
	for i := 0; i < p.ImageHeight; i++ {
		for j := 0; j < p.ImageWidth; j++ {
			if world[i][j] == byte(255) {
				c.ioOutput <- byte(255)
			} else {
				c.ioOutput <- byte(0)
			}
		}
	}
	return filename
}

func waitOutputComplete(c distributorChannels) {
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
}

func handlingEvent(world [][]byte, round int, controlChan *controlChannels, c distributorChannels, finalTurns *int, p Params) bool {
	quit := true
	pause := false
	for {
		select {
		case <-controlChan.ticker.C:
			nums := aliveCounter(world, p)
			c.events <- AliveCellsCount{round + 1, nums}
		case <-controlChan.quit: // deal with exit
			*finalTurns = round + 1
			return true
		case <-controlChan.pause:
			pause = !pause
			if pause { // deal with pausing
				c.events <- StateChange{round + 1, Paused}
				quit = false
			} else { // deal with resuming
				c.events <- StateChange{round + 1, Executing}
				quit = true
			}
		case <-controlChan.save: // deal with saving
			filename := output(world, &c, p, round+1)
			waitOutputComplete(c)
			c.events <- ImageOutputComplete{CompletedTurns: round + 1, Filename: filename}
		default:
		}
		if quit {
			break
		}
	}
	return false
}

func aliveCounter(world [][]byte, p Params) int {
	result := 0
	var mutex sync.Mutex
	var wg sync.WaitGroup
	yInterval := p.ImageHeight / p.Threads
	ys := 0
	ye := yInterval
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
		ys += yInterval
		ye += yInterval
	}
	wg.Wait()
	return result
}

func checker(world [][]byte, p Params) []util.Cell {
	result := make([]util.Cell, 0)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	yInterval := p.ImageHeight / p.Threads
	ys := 0
	ye := yInterval
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
		ys += yInterval
		ye += yInterval
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
	c.events <- CellsFlipped{0, checker(world, p)}
	c.events <- StateChange{turn, Executing}

	controlChan := controlChannels{make(chan bool, 1), make(chan bool, 1), make(chan bool, 1), time.NewTicker(2 * time.Second)}
	// deal with keyboard input
	go keyBoardDetection(keyPresses, controlChan)
	//gameQuit := false
	//finalTurns := p.Turns
	// run evolution until end of turns
	for i := 0; i < p.Turns; i++ {
		world = centralDistributor(world, p, c, i)
		c.events <- TurnComplete{i + 1}
		//gameQuit = handlingEvent(world, i, &controlChan, c, &finalTurns, p)
		//if gameQuit {
		//	break
		//}
	}
	//filename = output(world, &c, p, finalTurns)
	//waitOutputComplete(c)
	//c.events <- ImageOutputComplete{CompletedTurns: finalTurns, Filename: filename}
	c.events <- FinalTurnComplete{p.Turns, checker(world, p)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- StateChange{turn, Quitting}
	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
