package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
)

type Operator struct{}

type Result struct {
	result [][]byte
	up     []byte
	down   []byte
}

var world [][]byte

var dir = [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}

var killChan = make(chan bool)

// these functions are used to simulate the turns of Game of Life
// single thread to detect surround point whether it is alive or die
func singleThreadWorker(world, result [][]byte) {
	height := len(world)
	width := len(world[0])
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[y][x] == byte(255) {
				for _, d := range dir {
					result[y+d[0]+1][(x+d[1]+width)%width] += 1
				}
			}
		}
	}
}

// multiple thread to detect surround point whether it is alive or die
func multiThreadWorker(partWorld [][]byte, result Result, wg *sync.WaitGroup) {
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
					result.result[height+d[0]-1][(x+d[1]+width)%width] += 1
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

func centralDistributor(world [][]byte, p Params) [][]byte {
	/*
		Received y, x world return y + 2, x result
	*/
	height := len(world) + 2
	width := len(world[0])

	result := make([][]byte, height)
	for i := 0; i < height; i++ {
		result[i] = make([]byte, width)
	}
	//according to the number of thread to choose method
	if p.Threads == 1 || p.ImageHeight/p.Threads < 4 {
		singleThreadWorker(world, result)
	} else {
		if p.Threads > 6 {
			p.Threads = 6
		}
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
				result: result[startY+1 : endY+1],
				up:     make([]byte, p.ImageWidth),
				down:   make([]byte, p.ImageWidth),
			}
			go multiThreadWorker(world[startY:endY], results[i], &wg)
		}
		wg.Wait()
		// combine up and down in result
		for i, r := range results {
			startY := interval * i
			endY := startY + interval
			if i == p.Threads-1 {
				endY = p.ImageHeight
			}
			for x := 0; x < p.ImageWidth; x++ {
				result[startY][x] += r.up[x]
				result[endY+1][x] += r.down[x]
			}
		}
	}
	return result
}

func (a *Operator) Kill(request NoParams, response *NoParams) error {
	killChan <- true
	return nil
}

// get counting matrix
func (a *Operator) DistributeWorker(request Request, response *Response) error {
	world = centralDistributor(request.World, request.P)
	// send world
	response.World = make([][]byte, len(world))
	for i := range world {
		response.World[i] = make([]byte, len(world[i]))
		copy(response.World[i], world[i])
	}
	return nil
}

// It's the sever, used to listen and pass the argus to client by channel
func server(port string) {
	err := rpc.Register(&Operator{})
	if err != nil {
		fmt.Println(err)
		return
	}
	listener, _ := net.Listen("tcp", port)
	fmt.Println("Server is listening on port", port)
	go func() {
		<-killChan
		// simulate interrupt signal
		fmt.Println("--- received killed signal ---")
		err = syscall.Kill(os.Getpid(), syscall.SIGINT)
		if err != nil {
			panic(err)
		}
	}()
	rpc.Accept(listener)
}
