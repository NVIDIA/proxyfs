package pfsagentConfig

import (
	"fmt"
)

func nextMenu(menuText string, menuOptions []string, menuOptionsTexts map[string]string) (userResponse string, err error) {
	fmt.Println(menuText)
	for {
		displayOptions(menuOptions, menuOptionsTexts)
		userResponse, err = getUserInput()
		// fmt.Printf("raw response: %v\n", response)
		if nil != err {
			fmt.Println("Error retrieving user input", err)
			continue
		}
		if len(userResponse) != 1 {
			fmt.Printf("Input should be a single character. You entered %v. Please try again\n", userResponse)
			continue
		}
		for _, validInput := range menuOptions {
			if validInput == userResponse {
				return menuOptionsTexts[userResponse], nil
			}

		}
		fmt.Printf("Your input is not valid: '%v'. Options are: %v. Please try again\n\n", userResponse, menuOptions)
	}
	// return "", fmt.Errorf("Something went wrong trying to read user input")
}

func displayOptions(optionsList []string, optionsTextsMap map[string]string) {
	for _, option := range optionsList {
		fmt.Printf("%v:\t%v\n", option, optionsTextsMap[option])
	}
	fmt.Printf("\nYour Selection: ")
}
