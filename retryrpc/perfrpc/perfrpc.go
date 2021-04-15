package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

// A subcommandAction is a function that implements a subcommand.
//
type subcommandAction func(name string, flagSet *flag.FlagSet, ipaddr string, port int) (err error)

// A subcommandInfo holds the flags that a subcommand accepts and a function
// that performs the subcommand's action.  After parsing, flagSet will include
// all the flags and arguments to the subcommand.
//
type subcommandInfo struct {
	flagSet *flag.FlagSet
	action  subcommandAction
}

// A subcommandInfoMap is a map from the subcommand name to the subcommand
// information for each of the subcommands.
//
type subcommandInfoMap map[string]subcommandInfo

// Add a subcommand to a subcommandInfoMap and return the flagSet created to
// parse its options.
//
func addSubcommand(subcmdMap subcommandInfoMap, cmdName string, subcmd string, action subcommandAction) (
	flagSet *flag.FlagSet) {

	flagSet = flag.NewFlagSet(cmdName+" "+subcmd, flag.ExitOnError)

	subcmdMap[subcmd] = subcommandInfo{
		flagSet: flagSet,
		action:  action,
	}
	return
}

// Print a usage message for this command and exit (all of the subcommand info
// must already be filled in first).
//
func Usage(cmdName string, subcmdMap map[string]subcommandInfo) {

	fmt.Fprintf(os.Stderr, "Usage: %s <subcommand> [options] [args]\n", cmdName)
	for _, subcmdInfo := range subcmdMap {
		subcmdInfo.flagSet.Usage()
	}
	os.Exit(2)
}

// TODO - call code in client.go
//
// pass count of messages, warm up like Craig said..... what other options?
// how dump bucketstats??
func clientCmd(subcmd string, flagSet *flag.FlagSet, ipAddr string, port int) (err error) {
	if flagSet.NArg() < 4 {
		err = fmt.Errorf("Must pass ipaddr, port, clients and messages")
		fmt.Printf("NArg: %v\n", flagSet.NArg())
		return
	}
	// TODO - clients, messages, proper checking?

	c := flagSet.Lookup("clients").Value
	clientCnt, _ := strconv.Atoi(c)
	m := flagSet.Lookup("messages").Value.String()
	messageCnt, _ := strconv.Atoi(m)

	fmt.Printf("clientCnt: %v messages: %v\n", clientCnt, messageCnt)

	/*
		parallelClientSenders(ipAddr, port, count)
	*/
	return
}

// TODO - how dump bucketstats, etc???? do from webserver?
func serverCmd(subcmd string, flagSet *flag.FlagSet, ipAddr string, port int) (err error) {
	becomeAServer(ipAddr, int, true)
	return
}

// Parse the command line and perform the requested subcommand.
//
func main() {
	// invocation name (typically "perfrpc")
	cmdName := os.Args[0]

	// subcmdMap is a map of each subcommand to its options and action
	subcmdMap := make(map[string]subcommandInfo)

	var (
		err      error
		flagSet  *flag.FlagSet
		ipAddr   *string
		port     *int
		clients  *int
		messages *int
	)

	// client subcommand
	flagSet = addSubcommand(subcmdMap, cmdName, "client", clientCmd)
	port = flagSet.Int("port", 0, "port to use to connect to server (required)")
	ipAddr = flagSet.String("ipaddr", "", "IP address to use to connect to server (required)")
	clients = flagSet.Int("clients", 0, "number of clients (required)")
	messages = flagSet.Int("messages", 0, "number of messages to send (required)")

	// server subcommand
	flagSet = addSubcommand(subcmdMap, cmdName, "server", serverCmd)
	port = flagSet.Int("port", 0, "port to use to connect to server (required)")
	ipAddr = flagSet.String("ipaddr", "", "IP address to use to connect to server (required)")

	// verify that a subcommand has been provided
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "%s: subcommand required\n", cmdName)
		Usage(cmdName, subcmdMap)
		os.Exit(2)
	}

	// lookup the subcommand
	subcmd := os.Args[1]
	subcmdInfo, ok := subcmdMap[subcmd]
	if !ok {
		fmt.Fprintf(os.Stderr, "%s: unknown subcommand '%s'\n", cmdName, subcmd)
		Usage(cmdName, subcmdMap)
		os.Exit(2)
	}
	flagSet = subcmdInfo.flagSet

	fmt.Printf("os.Args: %v\n", os.Args)
	fmt.Printf("os.Args[2:]: %v\n", os.Args[2:])

	// parse the command line based on the subcommands' flags;
	// os.Args[2:] will be all arguments starting after the subcommand at os.Args[1]
	err = flagSet.Parse(os.Args[2:])
	if err != nil {
		panic(fmt.Sprintf("Parse returned an error (should have exited): %v", err))
	}

	fmt.Printf("flagSet: %+v\n", flagSet)

	// Verify that the user specified the IP address and port
	if *port == 0 {
		fmt.Println("The 'port <port number used by server>' option is not optional -- must specify a port number")
		flagSet.Usage()
		os.Exit(1)
	}

	if *ipAddr == "" {
		fmt.Println("The 'ipaddr <IP Address of server>' option is not optional -- must specify an IP Address")
		flagSet.Usage()
		os.Exit(1)
	}
	fmt.Printf("main() flagSet.NArg(): %v \n", flagSet.NArg())

	// invoke the action for the subcommand
	err = subcmdMap[subcmd].action(subcmd, flagSet, *ipAddr, *port)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s error: %s\n", cmdName, subcmd, err)
		os.Exit(1)
	}
	os.Exit(0)
}
