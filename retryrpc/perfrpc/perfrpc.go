package main

import (
	"errors"
	"fmt"
	"os"
)

// Interface defining the subcommands for this command
//
// Allows us to organize all variables for a subcommand in one place
//
type Runner interface {
	Init([]string) error // Based on subcommand - init arguments before parsing
	Name() string        // Return name of subcommand
	Run() error          // Perform subcommand
}

// Find subcommand, init flagSet, parse and and run subcommand
func main() {
	var (
		err error
	)

	if len(os.Args) < 2 {
		err = errors.New("You must pass a sub-command - either 'client' or 'server'")
		fmt.Println(err)
		os.Exit(1)
	}

	cmds := []Runner{
		NewClientCommand(),
		NewServerCommand(),
	}

	subcommand := os.Args[1]
	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(os.Args[2:])
			err = cmd.Run()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	err = fmt.Errorf("Unknown subcommand: %s - must be either 'client' or 'server'", subcommand)
	fmt.Println(err)
	os.Exit(1)

}
