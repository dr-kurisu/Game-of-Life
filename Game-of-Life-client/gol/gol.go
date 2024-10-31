package gol

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It should initialise channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	//	TODO: Put the missing channels in here.
	nameChan := make(chan string, 20)
	inputChan := make(chan byte, 20)
	ioOutput := make(chan byte, 20)

	ioCommand := make(chan ioCommand)
	ioIdle := make(chan bool)

	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: nameChan,
		output:   ioOutput,
		input:    inputChan,
	}
	go startIo(p, ioChannels)

	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: nameChan,
		ioOutput:   ioOutput,
		ioInput:    inputChan,
	}
	distributor(p, distributorChannels, keyPresses)
}
