package pfsagentConfig

import (
	"fmt"
	"os"
)

// RunStateMachine run the state machine controlling the wizard flow
func RunStateMachine() (err error) {
	// fmt.Println("runWizard starting")
	prevMenuText := new(stringStack)
	prevMenuOptions := new(stringArrayStack)
	prevMenuOptionsTexts := new(stringMapStack)
	prevMenuText.push(mainMenuText)
	prevMenuOptions.push(mainMenuOptions)
	prevMenuOptionsTexts.push(mainMenuOptionsTexts)

	nextMenuText := mainMenuText
	nextMenuOptions := mainMenuOptions
	nextMenuOptionsTexts := mainMenuOptionsTexts
	for {
		menuResponse, displayErr := nextMenu(nextMenuText, nextMenuOptions, nextMenuOptionsTexts)
		// fmt.Printf("menuResponse: %v\n", menuResponse)
		if nil != displayErr {
			// fmt.Println("ERROR while displaying menu item", displayErr)
			err = fmt.Errorf("error trying to display menu item")
			return
		}
		switch menuResponse {
		case quitMenuOptionText:
			fmt.Println("Thank you for using the pfsagent config util")
			return
		case backMenuOptionText:
			nextMenuText = prevMenuText.pop()
			nextMenuOptions = prevMenuOptions.pop()
			nextMenuOptionsTexts = prevMenuOptionsTexts.pop()
		case changeCredsOptionText:
			// fmt.Printf("got %v\n", changeCredsOptionText)
			prevMenuText.push(nextMenuText)
			prevMenuOptions.push(nextMenuOptions)
			prevMenuOptionsTexts.push(nextMenuOptionsTexts)
			nextMenuText = credentialsMenuTexts
			nextMenuOptions = credentialsMenuOptions
			nextMenuOptionsTexts = credentialsMenuOptionsTexts
		case changeOtherOptionText:
			fmt.Printf("got %v\n", changeOtherOptionText)

		case changeAuthURLOptionText:
			userResponse, userInputErr := getValueFromUser("Swift Auth URL", "", confMap["Agent"]["SwiftAuthURL"][0])
			if nil != userInputErr {
				fmt.Printf(userInputErrorMessage, userInputErr)
				return userInputErr
			}
			prevAuthURL := confMap["Agent"]["SwiftAuthURL"][0]
			confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthURL", userResponse))
			whatFailed, accessErr := ValidateAccess()
			if nil != accessErr {
				switch whatFailed {
				case typeAuthURL:
					confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthURL", prevAuthURL))
					fmt.Printf(failureMessageHeader)
					fmt.Printf(authURLFailedMessage, accessErr)
					fmt.Printf(failureMessageFooter)
				case typeCredentails:
					fmt.Printf(needMoreInfoMessageHeader)
					fmt.Printf(credentialsFailedMessage, confMap["Agent"]["SwiftAuthUser"][0], confMap["Agent"]["SwiftAuthKey"][0], accessErr)
					SaveCurrentConfig()
					fmt.Printf(authURLSetMessage, confMap["Agent"]["SwiftAuthURL"][0])
					fmt.Printf(needMoreInfoMessageFooter)
				case typeAccount:
					fmt.Printf(needMoreInfoMessageHeader)
					fmt.Printf(accountFailedMessage, confMap["Agent"]["SwiftAccountName"][0], confMap["Agent"]["SwiftAuthUser"][0], accessErr)
					SaveCurrentConfig()
					fmt.Printf(authURLSetMessage, confMap["Agent"]["SwiftAuthURL"][0])
					fmt.Println(changesSavedMessage)
					fmt.Printf(needMoreInfoMessageFooter)
				}
			} else {
				fmt.Printf(successMessageHeader)
				fmt.Printf(accessCheckSucceededMessage)
				fmt.Printf(authURLSetMessage, confMap["Agent"]["SwiftAuthURL"][0])
				SaveCurrentConfig()
				fmt.Println(changesSavedMessage)
				fmt.Printf(successMessageFooter)
				nextMenuText = mainMenuText
				nextMenuOptions = mainMenuOptions
				nextMenuOptionsTexts = mainMenuOptionsTexts
			}

		case changeUsernameOptionText:
			userResponse, userInputErr := getValueFromUser("Swift Username", "", confMap["Agent"]["SwiftAuthUser"][0])
			if nil != userInputErr {
				fmt.Printf(userInputErrorMessage, userInputErr)
				return userInputErr
			}
			prevAuthUser := confMap["Agent"]["SwiftAuthUser"][0]
			confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthUser", userResponse))
			whatFailed, accessErr := ValidateAccess()
			if nil != accessErr {
				switch whatFailed {
				case typeAuthURL:
					confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthUser", prevAuthUser))
					fmt.Printf(failureMessageHeader)
					fmt.Printf(authURLFailedMessage, accessErr)
					fmt.Printf(failureMessageFooter)
				case typeCredentails:
					fmt.Printf(userSetMessage, confMap["Agent"]["SwiftAuthUser"][0])
					SaveCurrentConfig()
					fmt.Println(changesSavedMessage)
					fmt.Printf(failureMessageHeader)
					fmt.Printf(credentialsFailedMessage, userResponse, confMap["Agent"]["SwiftAuthKey"][0], accessErr)
					fmt.Printf(failureMessageFooter)
				case typeAccount:
					fmt.Printf(needMoreInfoMessageHeader)
					fmt.Printf(accountFailedMessage, confMap["Agent"]["SwiftAccountName"][0], confMap["Agent"]["SwiftAuthUser"][0], accessErr)
					// MyConfig.SwiftAuthUser = userResponse
					fmt.Printf(userSetMessage, confMap["Agent"]["SwiftAuthUser"][0])
					SaveCurrentConfig()
					fmt.Println(changesSavedMessage)
					fmt.Printf(needMoreInfoMessageFooter)
				}
			} else {
				fmt.Printf(successMessageHeader)
				fmt.Printf(accessCheckSucceededMessage)
				// MyConfig.SwiftAuthUser = userResponse
				fmt.Printf(userSetMessage, confMap["Agent"]["SwiftAuthUser"][0])
				SaveCurrentConfig()
				fmt.Println(changesSavedMessage)
				fmt.Printf(successMessageFooter)
				nextMenuText = mainMenuText
				nextMenuOptions = mainMenuOptions
				nextMenuOptionsTexts = mainMenuOptionsTexts
			}

		case changeKeyOptionText:
			userResponse, userInputErr := getValueFromUser("Swift User Key", "", confMap["Agent"]["SwiftAuthKey"][0])
			if nil != userInputErr {
				fmt.Printf(userInputErrorMessage, userInputErr)
				return userInputErr
			}
			prevAuthKey := confMap["Agent"]["SwiftAuthKey"][0]
			confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthKey", userResponse))
			whatFailed, accessErr := ValidateAccess()
			if nil != accessErr {
				switch whatFailed {
				case typeAuthURL:
					confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthKey", prevAuthKey))
					fmt.Printf(failureMessageHeader)
					fmt.Printf(authURLFailedMessage, accessErr)
					fmt.Printf(failureMessageFooter)
				case typeCredentails:
					// MyConfig.SwiftAuthKey = prevAuthKey
					fmt.Printf(keySetMessage, confMap["Agent"]["SwiftAuthKey"][0])
					SaveCurrentConfig()
					fmt.Println(changesSavedMessage)
					fmt.Printf(failureMessageHeader)
					fmt.Printf(credentialsFailedMessage, confMap["Agent"]["SwiftAuthUser"][0], userResponse, accessErr)
					fmt.Printf(failureMessageFooter)
				case typeAccount:
					fmt.Printf(needMoreInfoMessageHeader)
					fmt.Printf(accountFailedMessage, confMap["Agent"]["SwiftAccountName"][0], confMap["Agent"]["SwiftAuthUser"][0], accessErr)
					// MyConfig.SwiftAuthKey = userResponse
					fmt.Printf(keySetMessage, confMap["Agent"]["SwiftAuthKey"][0])
					SaveCurrentConfig()
					fmt.Println(changesSavedMessage)
					fmt.Printf(needMoreInfoMessageFooter)
				}
			} else {
				fmt.Printf(successMessageHeader)
				fmt.Printf(accessCheckSucceededMessage)
				// MyConfig.SwiftAuthKey = userResponse
				fmt.Printf(keySetMessage, confMap["Agent"]["SwiftAuthKey"][0])
				SaveCurrentConfig()
				fmt.Println(changesSavedMessage)
				fmt.Printf(successMessageFooter)
				nextMenuText = mainMenuText
				nextMenuOptions = mainMenuOptions
				nextMenuOptionsTexts = mainMenuOptionsTexts
			}

		case changeAccountOptionText:
			userResponse, userInputErr := getValueFromUser("Swift Account", "", confMap["Agent"]["SwiftAccountName"][0])
			if nil != userInputErr {
				fmt.Printf(userInputErrorMessage, userInputErr)
				return userInputErr
			}
			prevAccountName := confMap["Agent"]["SwiftAccountName"][0]
			confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAccountName", userResponse))
			whatFailed, accessErr := ValidateAccess()
			if nil != accessErr {
				fmt.Printf(failureMessageHeader)
				confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAccountName", prevAccountName))
				switch whatFailed {
				case typeAuthURL:
					fmt.Printf(authURLFailedMessage, accessErr)
				case typeCredentails:
					fmt.Printf(credentialsFailedMessage, confMap["Agent"]["SwiftAuthUser"][0], confMap["Agent"]["SwiftAuthKey"][0], accessErr)
				case typeAccount:
					fmt.Printf(accountFailedMessage, confMap["Agent"]["SwiftAccountName"][0], confMap["Agent"]["SwiftAuthUser"][0], accessErr)
				}
				fmt.Printf(failureMessageFooter)
			} else {
				fmt.Printf(successMessageHeader)
				fmt.Printf(accessCheckSucceededMessage)
				// MyConfig.SwiftAccountName = userResponse
				fmt.Printf(accountSetMessage, confMap["Agent"]["SwiftAccountName"][0])

				SaveCurrentConfig()
				fmt.Println(changesSavedMessage)
				fmt.Printf(successMessageFooter)
				nextMenuText = mainMenuText
				nextMenuOptions = mainMenuOptions
				nextMenuOptionsTexts = mainMenuOptionsTexts
			}

		default:
			fmt.Printf("got unknown response: %v\n", menuResponse)
		}
	}
}

// FirstTimeRun is to be called for the first (initial) run of pfsagentConfig
// to create an initial real config
func FirstTimeRun() error {
	loadError := LoadConfig("")
	if nil != loadError {
		fmt.Println("Failed loading config. Error:", loadError)
		os.Exit(1)
	}

	var oldAuthURL string
	var oldAuthUser string
	var oldAuthKey string
	var oldAccount string
	var oldMount string
	var oldVolName string
	var oldLogPath string

	if len(confMap["Agent"]["SwiftAuthURL"]) > 0 {
		oldAuthURL = confMap["Agent"]["SwiftAuthURL"][0]
	}
	if len(confMap["Agent"]["SwiftAuthUser"]) > 0 {
		oldAuthUser = confMap["Agent"]["SwiftAuthUser"][0]
	}
	if len(confMap["Agent"]["SwiftAuthKey"]) > 0 {
		oldAuthKey = confMap["Agent"]["SwiftAuthKey"][0]
	}
	if len(confMap["Agent"]["SwiftAccountName"]) > 0 {
		oldAccount = confMap["Agent"]["SwiftAccountName"][0]
	}
	if len(confMap["Agent"]["FUSEMountPointPath"]) > 0 {
		oldMount = confMap["Agent"]["FUSEMountPointPath"][0]
	}
	if len(confMap["Agent"]["FUSEVolumeName"]) > 0 {
		oldVolName = confMap["Agent"]["FUSEVolumeName"][0]
	}
	if len(confMap["Agent"]["LogFilePath"]) > 0 {
		oldLogPath = confMap["Agent"]["LogFilePath"][0]
	}

	fmt.Println(firstTimeCredentialsMenu)
	mySwiftParams := new(SwiftParams)

	me := confMap["Agent"]
	fmt.Println(me)
	// validAuthURL := false
	for {
		userURLResponse, userURLInputErr := getValueFromUser("Swift Auth URL", authURLHint, "")
		fmt.Println()
		if nil != userURLInputErr {
			return fmt.Errorf(userInputErrorMessage, userURLInputErr)
		}
		mySwiftParams.AuthURL = userURLResponse
		userURLValidateErr := validateURL(mySwiftParams)
		if nil != userURLValidateErr {
			fmt.Printf(failureMessageHeader)
			fmt.Printf("%v\n\n", userURLValidateErr)
			fmt.Printf(failureMessageFooter)
		} else {
			confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthURL", userURLResponse))
			break
		}
	}

	for {
		userUserResponse, userUserInputErr := getValueFromUser("Swift Auth User", usernameHint, "")
		fmt.Println()
		if nil != userUserInputErr {
			return fmt.Errorf(userInputErrorMessage, userUserInputErr)
		}

		userKeyResponse, userKeyInputErr := getValueFromUser("Swift Auth Key", keyHint, "")
		fmt.Println()
		if nil != userKeyInputErr {
			return fmt.Errorf(userInputErrorMessage, userKeyInputErr)
		}
		mySwiftParams.User = userUserResponse
		mySwiftParams.Key = userKeyResponse

		token, credValidationErr := validateCredentails(mySwiftParams)
		if nil != credValidationErr {
			fmt.Printf(failureMessageHeader)
			fmt.Printf("%v\n\n", credValidationErr)
			fmt.Printf(failureMessageFooter)
		} else {
			confMap.UpdateFromStrings([]string{
				fmt.Sprintf("%v : %v", "Agent.SwiftAuthUser", userUserResponse),
				fmt.Sprintf("%v : %v", "Agent.SwiftAuthKey", userKeyResponse),
			})
			mySwiftParams.AuthToken = token
			break
		}
	}
	fmt.Printf("mySwiftParams: %v\n", mySwiftParams)

	validAccount := false
	for !validAccount {
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAccountName", fmt.Sprintf("AUTH_%v", confMap["Agent"]["SwiftAuthUser"][0])))

		userAccountResponse, userAccountInputErr := getValueFromUser("Swift Account", accountHint, confMap["Agent"]["SwiftAccountName"][0])
		fmt.Println()
		if nil != userAccountInputErr {
			return fmt.Errorf(userInputErrorMessage, userAccountInputErr)
		}
		if len(userAccountResponse) == 0 {
			userAccountResponse = confMap["Agent"]["SwiftAccountName"][0]
		}

		mySwiftParams.Account = userAccountResponse
		accountValidationErr := validateAccount(mySwiftParams)
		if nil != accountValidationErr {
			fmt.Printf(failureMessageHeader)
			fmt.Printf("%v\n\n", accountValidationErr)
			fmt.Printf(failureMessageFooter)
			mySwiftParams.StorageURL = ""
		} else {
			confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAccountName", userAccountResponse))
			validAccount = true
		}
	}

	suggestedMount := fmt.Sprintf("%v/vol_%v", defaultMountPath, confMap["Agent"]["SwiftAccountName"][0])
	suggestedLogs := fmt.Sprintf("%v/log.%v", defaultLogPath, confMap["Agent"]["SwiftAccountName"][0])

	confMap.UpdateFromStrings([]string{
		fmt.Sprintf("%v : %v", "Agent.LogFilePath", suggestedLogs),
		fmt.Sprintf("%v : %v", "Agent.FUSEMountPointPath", suggestedMount),
		fmt.Sprintf("%v : %v", "Agent.FUSEVolumeName", confMap["Agent"]["SwiftAccountName"][0]),
	})

	volNameResponse, volNameInputErr := getValueFromUser("Volume Name", volNameHint, confMap["Agent"]["SwiftAccountName"][0])
	fmt.Println()
	if nil != volNameInputErr {
		return fmt.Errorf(userInputErrorMessage, volNameInputErr)
	}
	if len(volNameResponse) > 0 {
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.FUSEVolumeName", volNameResponse))
	}

	mountPathResponse, mountPathInputErr := getValueFromUser("Mount Point", mountPointHint, confMap["Agent"]["FUSEMountPointPath"][0])
	fmt.Println()
	if nil != mountPathInputErr {
		return fmt.Errorf(userInputErrorMessage, mountPathInputErr)
	}
	if len(mountPathResponse) > 0 {
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.FUSEMountPointPath", mountPathResponse))
	}

	whatFailed, accessErr := ValidateAccess()
	if nil != accessErr {
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthURL", oldAuthURL))
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthUser", oldAuthUser))
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAuthKey", oldAuthKey))
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.SwiftAccountName", oldAccount))
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.FUSEMountPointPath", oldMount))
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.FUSEVolumeName", oldVolName))
		confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.LogFilePath", oldLogPath))
		fmt.Printf(failureMessageHeader)
		switch whatFailed {
		case typeAuthURL:
			fmt.Printf(authURLFailedMessage, accessErr)
		case typeCredentails:
			fmt.Printf(credentialsFailedMessage, confMap["Agent"]["SwiftAuthUser"][0], confMap["Agent"]["SwiftAuthKey"][0], accessErr)
			fmt.Printf(authURLSetMessage, confMap["Agent"]["SwiftAuthURL"][0])
		case typeAccount:
			fmt.Printf(accountFailedMessage, confMap["Agent"]["SwiftAccountName"][0], confMap["Agent"]["SwiftAuthUser"][0], accessErr)
			fmt.Printf(authURLSetMessage, confMap["Agent"]["SwiftAuthURL"][0])
			fmt.Println(changesSavedMessage)
		}
		fmt.Printf(failureMessageFooter)
	} else {
		fmt.Printf(successMessageHeader)
		fmt.Println(accessCheckSucceededMessage)

		if _, err := os.Stat(confMap["Agent"]["LogFilePath"][0]); os.IsNotExist(err) {
			err = os.MkdirAll(confMap["Agent"]["LogFilePath"][0], 0755)
			if err != nil {
				fmt.Printf(failureMessageHeader)
				panic(err)
			}
		}

		if _, err := os.Stat(confMap["Agent"]["FUSEMountPointPath"][0]); os.IsNotExist(err) {
			err = os.MkdirAll(confMap["Agent"]["FUSEMountPointPath"][0], 0755)
			if err != nil {
				fmt.Printf(failureMessageHeader)
				panic(err)
			}
		}
		SaveCurrentConfig()
		fmt.Println(changesSavedMessage)
		fmt.Printf(successMessageFooter)
	}

	return nil
}
