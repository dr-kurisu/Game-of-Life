package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/communication"
)

type GolAttribute struct{}

//this function is used to simulate the turns of Game of Life
func counting(world [][]byte, x, y int) int {
	dir := [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}
	counter := 0
	width := len(world[0])
	height := len(world)
	for _, d := range dir {
		if y+d[0] < 0 {
			y = height - 1
		}
		if x+d[1] < 0 {
			x = width - 1
		}
		if y+d[0] >= height {
			y = 0
		}
		if x+d[1] >= width {
			x = 0
		}

		if world[y][x] == byte(255) {
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
	for y, _ := range world {
		for x, _ := range world[y] {
			nums := counting(world, x, y)
			switch {
			case nums == 3:
				new_world[y][x] = byte(255)
			case nums > 3 || nums < 2:
				new_world[y][x] = byte(0)
			default:
				new_world[y][x] = world[y][x]
			}

		}
	}
	return new_world
}

func (a *GolAttribute) connectionHandler(server communication.ReceiveFromServer, reply *communication.SendToClient) {
	p := server.P
	world := server.CurrentWorld
	// run evolution until end of turns
	for i := 0; i < p.Turns; i++ {
		world = evolution(world)
	}
	reply.World = world
}

//It's the sever, used to listen and pass the argus to client by channel
func main() {
	Address := flag.String("port", ":8030", "client Port")
	flag.Parse()
	rpc.Register(GolAttribute{})
	listener, _ := net.Listen("tcp", *Address)
	fmt.Println("Server is listening on port", *Address)
	conn, _ := listener.Accept()
	defer conn.Close()
	rpc.Accept(listener)
}
