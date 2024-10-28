package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/util"
)

type Operator struct{}

//this function is used to simulate the turns of Game of Life
func counting(world [][]byte, x, y int) int {
	dir := [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}
	counter := 0
	width := len(world[0])
	height := len(world)
	for _, d := range dir {
		newY := (y + d[0] + height) % height
		newX := (x + d[1] + width) % width
		if world[newY][newX] == byte(255) {
			counter++
		}
	}
	return counter
}

func evolution(world [][]byte) [][]byte {
	newWorld := make([][]byte, len(world))
	for i := 0; i < len(world); i++ {
		newWorld[i] = make([]byte, len(world[0]))
	}
	for y, _ := range world {
		for x, _ := range world[y] {
			nums := counting(world, x, y)
			switch {
			case nums == 3:
				newWorld[y][x] = byte(255)
			case nums > 3 || nums < 2:
				newWorld[y][x] = byte(0)
			default:
				newWorld[y][x] = world[y][x]
			}
		}
	}
	return newWorld
}

func (a *Operator) Connection(request ReceiveFromServer, reply *SendToClient) error {
	fmt.Println("round Start")
	p := request.P
	reply.World = make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		reply.World[i] = make([]byte, p.ImageWidth)
	}
	world := make([][]byte, request.P.ImageHeight)
	for i := 0; i < request.P.ImageHeight; i++ {
		world[i] = make([]byte, request.P.ImageWidth)
		for j := 0; j < request.P.ImageWidth; j++ {
			world[i][j] = request.CurrentWorld[i][j]
		}
	}
	// run evolution until end of turns
	world = evolution(world)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			reply.World[y][x] = world[y][x]
		}
	}
	return nil
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

//It's the sever, used to listen and pass the argus to client by channel
func server() {
	err := rpc.Register(&Operator{})
	if err != nil {
		fmt.Println("register error")
		return
	}
	listener, _ := net.Listen("tcp", ":8030")
	fmt.Println("Server is listening on port", "8030")
	rpc.Accept(listener)
}
