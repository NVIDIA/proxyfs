package pfsagentConfig

import (
	"fmt"
	"net/http"
	"strings"
)

// SwiftParams is a struct to hold all parameters needed to connect to a swift account
type SwiftParams struct {
	// Account is the account name
	Account string
	// User is the username
	User string
	// Key is the username's key (password)
	Key string
	// AuthURL is the URL to get an auth token with
	AuthURL string
	// StorageURL is the URL for data access
	StorageURL string
	// AuthToken is the token used for data access
	AuthToken string
}

// ConstructStorageURL calculates the storage URL based on the auth URL and the account name
func ConstructStorageURL(mySwiftParams *SwiftParams) (storageURL string, err error) {
	upTo := strings.Index(mySwiftParams.AuthURL, "/auth")
	if upTo < 0 {
		err = fmt.Errorf("Could Not Construct Storage URL From Auth URL %v. Missing '/auth/ in URL'", mySwiftParams.AuthURL)
		return
	}
	storageURL = fmt.Sprintf("%v/v1/%v", mySwiftParams.AuthURL[:upTo], mySwiftParams.Account)
	return
}

// GetAuthToken retrieves the auth token for the user/key combination
func (cfg PFSagentConfig) GetAuthToken(mySwiftParams *SwiftParams) (authToken string, err error) {
	authClient := &http.Client{}
	authTokenRequest, err := http.NewRequest("GET", mySwiftParams.AuthURL, nil)
	if nil != err {
		fmt.Printf("tokenErr:\n%v\n", err)
		return
	}
	authTokenRequest.Header.Add("x-auth-user", mySwiftParams.User)
	authTokenRequest.Header.Add("x-auth-key", mySwiftParams.Key)
	// authTokenRequest.Header.Add("Content-Type", "application/json")
	// fmt.Printf("auth request:\n%v\n", authTokenRequest)
	authTokenResponse, authErr := authClient.Do(authTokenRequest)
	// fmt.Printf("authTokenResponse: %v", authTokenResponse)
	if nil != authErr {
		err = fmt.Errorf("Error executing auth token request:\n\tError: %v", authErr)
		return
	}
	defer authTokenResponse.Body.Close()
	if authTokenResponse.StatusCode > 400 {
		err = fmt.Errorf("Error authenticating for auth token. Check your user and key:\n\tStatus code: %v", authTokenResponse.Status)
		return
	}
	if authTokenResponse.StatusCode > 299 {
		err = fmt.Errorf("Error response status code from %v\n\tStatus code: %v", cfg.confMap["Agent"]["SwiftAuthURL"][0], authTokenResponse.StatusCode)
		return
	}
	authToken = authTokenResponse.Header.Get("X-Auth-Token")
	// authTokenBody, authTokenBodyReadErr := ioutil.ReadAll(authTokenResponse.Body)
	if len(authToken) == 0 {
		err = fmt.Errorf("Error finding auth token in response from %v\n\tResponse: %v", cfg.confMap["Agent"]["SwiftAuthURL"][0], authTokenResponse)
		return
	}
	// fmt.Printf("auth token:\n%v\n", authToken)
	return
}

// GetAccountHeaders retrieves the headers for an account
func (cfg PFSagentConfig) GetAccountHeaders(mySwiftParams *SwiftParams) (headers http.Header, err error) {
	if len(mySwiftParams.StorageURL) == 0 {
		tempStorageURL, urlErr := ConstructStorageURL(mySwiftParams)
		if nil != urlErr {
			err = urlErr
			return
		}
		mySwiftParams.StorageURL = tempStorageURL
	}
	// fmt.Printf("mySwiftParams.StorageURL: %v\n", mySwiftParams.StorageURL)

	if len(mySwiftParams.AuthToken) == 0 {
		tempAuthToken, authTokenErr := cfg.GetAuthToken(mySwiftParams)
		if nil != authTokenErr {
			err = authTokenErr
			return
		}
		mySwiftParams.AuthToken = tempAuthToken
	}
	headClient := &http.Client{}
	accountHeadRequest, accountHeadErr := http.NewRequest("HEAD", mySwiftParams.StorageURL, nil)
	if nil != accountHeadErr {
		err = fmt.Errorf("Error executing HEAD request:\n\tError: %v", accountHeadErr)
		return
	}
	accountHeadRequest.Header.Add("x-auth-token", mySwiftParams.AuthToken)
	accountHeadResponse, accountHeadErr := headClient.Do(accountHeadRequest)

	// fmt.Printf("accountHeadResponse: %v", accountHeadResponse)
	if nil != accountHeadErr {
		err = fmt.Errorf("Error executing HEAD request:\n\tError: %v", accountHeadErr)
		return
	}
	if accountHeadResponse.StatusCode >= 400 {
		err = fmt.Errorf("Bad Status Code Getting Account Headers:\n\tStatus Code: %v", accountHeadResponse.StatusCode)
		return
	}
	// fmt.Printf("accountHeadResponse: %v", accountHeadResponse)
	headers = accountHeadResponse.Header
	return
}
