package pfsagentConfig

const (
	typeAuthURL int = iota
	typeCredentails
	typeAccount
)

const (
	Version = "0.0.1"

	mainMenuText = `
This is a utility to set the parameters for your PFSagentd. It can help you set/modify the access credentials or change any runtime parameters.
Please choose the right option from the menu below:
`

	firstTimeCredentialsMenu = `It seems like this is the first time, so let's run through everything:`

	credentialsMenuTexts = `
This menu lets you set the access parameters.
`

	mainMenuOptionText = "Return To Main Menu"
	quitMenuOptionText = "Quit"
	backMenuOptionText = "Back"

	changeCredsOptionText = "Change Access Credentials"
	changeOtherOptionText = "Change Other Parameters"

	changeAuthURLOptionText   = "Change Auth URL"
	changeUsernameOptionText  = "Change Username"
	changeKeyOptionText       = "Change User Key"
	changeAccountOptionText   = "Change Account"
	changeMountOptionText     = "Change Mount Point"
	successMessageHeader      = "\n\n++++++++++++++++++++++++++    SUCCESS    ++++++++++++++++++++++++++\n\n"
	successMessageFooter      = "\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n"
	failureMessageHeader      = "\n\n**************************     ERROR     **************************\n\n"
	failureMessageFooter      = "\n*******************************************************************\n\n"
	needMoreInfoMessageHeader = "\n\n//////////////////////////   ATTENTION   //////////////////////////\n\n"
	needMoreInfoMessageFooter = "\n///////////////////////////////////////////////////////////////////\n\n"
	authURLHint               = "The Auth URL Is Used To Authenticate And Get An Auth Token"
	usernameHint              = "A Valid User For The SwiftStack Cluster"
	keyHint                   = "A Valid Key (Password) For The SwiftStack Cluster User"
	accountHint               = "The Account To Log Into. Usually In This Form: AUTH_xxxx"
	volNameHint               = "The Name For This Volume .This Will Be Used To Identify This Configuration, As Well As The Mount Point And Log File"
	mountPointHint            = "The Mount Point For This Volume"
)

var (
	mainMenuOptions      = []string{"1", "2", "q"}
	mainMenuOptionsTexts = map[string]string{
		mainMenuOptions[0]: changeCredsOptionText,
		mainMenuOptions[1]: changeOtherOptionText,
		mainMenuOptions[2]: quitMenuOptionText,
	}
	credentialsMenuOptions      = []string{"1", "2", "3", "4", "b", "q"}
	credentialsMenuOptionsTexts = map[string]string{
		credentialsMenuOptions[0]: changeAuthURLOptionText,
		credentialsMenuOptions[1]: changeUsernameOptionText,
		credentialsMenuOptions[2]: changeKeyOptionText,
		credentialsMenuOptions[3]: changeAccountOptionText,
		credentialsMenuOptions[4]: mainMenuOptionText,
		credentialsMenuOptions[5]: quitMenuOptionText,
	}
)

const (
	userInputErrorMessage       = "Error Reading Input From User\n%v"
	authURLFailedMessage        = "Auth URL Failed, So I Could Not Check User And Key. Please Verify Auth URL\n%v\n\n"
	credentialsFailedMessage    = "Auth URL Works, But I Got An Error Trying To Login With Credentails\nUser: %v\nKey: %v\n%v\n\n"
	accountFailedMessage        = "Auth URL And Credentials Works, But I Could Not Gain Access To Account %v. Please Verify The Account Exists And User %v Has The Correct Access Permissions\n%v\n\n"
	changesSavedMessage         = "Changes Saved To File"
	accessCheckSucceededMessage = "All Access Checks Succeeded"
	accountSetMessage           = "Swift Account Set To %v\n"
	userSetMessage              = "Swift User Set To %v\n"
	keySetMessage               = "Swift User Key Set to %v\n"
	authURLSetMessage           = "Swift Auth URL Set To %v\n"
)
