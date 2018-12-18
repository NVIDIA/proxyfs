// The cleanproxyfs program deletes the headhunter databases and deletes log
// segments from Swift, thereby creating a "clean slate" for continued testing or
// development.
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	swiftAccountCheckpointHeaderName = "X-Account-Meta-Checkpoint"
)

func main() {
	cumulativeFailures := 0
	verbose := false

	args := os.Args[1:]

	// Check for verbose option in args[0]
	if (0 < len(args)) && ("-v" == args[0]) {
		verbose = true
		args = args[1:]
	}

	// Read in the program's args[0]-specified (and required) .conf file
	if 0 == len(args) {
		fmt.Fprint(os.Stderr, "no .conf file specified\n")
		os.Exit(1)
	}

	confMap, confErr := conf.MakeConfMapFromFile(args[0])
	if nil != confErr {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", confErr)
		os.Exit(1)
	}

	// Update confMap with any extra os.Args supplied
	confErr = confMap.UpdateFromStrings(args[1:])
	if nil != confErr {
		fmt.Fprintf(os.Stderr, "failed to load config overrides: %v\n", confErr)
		os.Exit(1)
	}

	// Upgrade confMap if necessary (remove when appropriate)
	//   Note that this follows UpdateFromStrings()
	//        hence overrides should be relative to the pre-upgraded .conf format
	confErr = transitions.UpgradeConfMapIfNeeded(confMap)
	if nil != confErr {
		fmt.Fprintf(os.Stderr, "failed to perform transitions.UpgradeConfMapIfNeeded(): %v\n", confErr)
		os.Exit(1)
	}

	// Fetch WhoAmI (qualifies which VolumeList elements are applicable)
	whoAmI, confErr := confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != confErr {
		fmt.Fprintf(os.Stderr, "confMap did not contain Cluster.WhoAmI\n")
		os.Exit(1)
	}

	// Compute activeVolumeNameList from each "active" VolumeGroup's VolumeList
	activeVolumeNameList := []string{}
	volumeGroupNameList, confErr := confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != confErr {
		fmt.Fprintf(os.Stderr, "confMap did not contain FSGlobals.VolumeGroupList\n")
		os.Exit(1)
	}
	for _, volumeGroupName := range volumeGroupNameList {
		volumeGroupSectionName := "VolumeGroup:" + volumeGroupName
		primaryPeerList, confErr := confMap.FetchOptionValueStringSlice(volumeGroupSectionName, "PrimaryPeer")
		if nil != confErr {
			fmt.Fprintf(os.Stderr, "confMap did not contain %v.PrimaryPeer\n", volumeGroupSectionName)
			os.Exit(1)
		}
		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if whoAmI == primaryPeerList[0] {
				volumeNameList, confErr := confMap.FetchOptionValueStringSlice(volumeGroupSectionName, "VolumeList")
				if nil != confErr {
					fmt.Fprintf(os.Stderr, "confMap did not contain %v.VolumeGroupList\n", volumeGroupSectionName)
					os.Exit(1)
				}
				activeVolumeNameList = append(activeVolumeNameList, volumeNameList...)
			} else {
				fmt.Fprintf(os.Stderr, "confMap contained multiple values for %v.PrimaryPeer: %v\n", volumeGroupSectionName, primaryPeerList)
				os.Exit(1)
			}
		}
	}

	// Clean out referenced Swift Accounts
	httpClient := &http.Client{}

	noAuthTCPPort, confErr := confMap.FetchOptionValueString("SwiftClient", "NoAuthTCPPort")
	if nil != confErr {
		fmt.Fprintf(os.Stderr, "confMap did not contain Swift.NoAuthTCPPort\n")
		os.Exit(1)
	}

	urlPrefix := "http://127.0.0.1:" + noAuthTCPPort + "/v1/"

	for _, volumeName := range activeVolumeNameList {
		volumeSectionName := "Volume:" + volumeName
		accountName, confErr := confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != confErr {
			fmt.Fprintf(os.Stderr, "confMap did not contain %v.AccountName\n", volumeSectionName)
			os.Exit(1)
		}

		accountURL := urlPrefix + accountName

		if verbose {
			fmt.Fprintf(os.Stdout, "about to clear out Volume %v @ Account %v\n", volumeName, accountURL)
		}

		accountGetResponse, accountGetErr := httpClient.Get(accountURL)
		if nil != accountGetErr {
			fmt.Fprintf(os.Stderr, "httpClient.Get(%v) returned unexpected error: %v\n", accountURL, accountGetErr)
			os.Exit(1)
		}

		if http.StatusNoContent == accountGetResponse.StatusCode {
			// Nothing to do here
		} else if http.StatusOK == accountGetResponse.StatusCode {
			accountGetResponseBodyByteSlice, accountGetResponseBodyReadAllErr := ioutil.ReadAll(accountGetResponse.Body)
			if nil != accountGetResponseBodyReadAllErr {
				fmt.Fprintf(os.Stderr, "ioutil.ReadAll(httpClient.Get(%v).Body)returned unexpected error: %v\n", accountURL, accountGetResponseBodyReadAllErr)
				os.Exit(1)
			}
			accountGetResponseBodyString := utils.ByteSliceToString(accountGetResponseBodyByteSlice)
			accountGetResponseBodyStringSlice := strings.Split(accountGetResponseBodyString, "\n")

			for _, containerName := range accountGetResponseBodyStringSlice[:len(accountGetResponseBodyStringSlice)-1] {
				containerURL := accountURL + "/" + containerName

				if verbose {
					fmt.Fprintf(os.Stdout, "about to clear out and remove %v\n", containerURL)
				}

				containerGetResponse, containerGetErr := httpClient.Get(containerURL)
				if nil != containerGetErr {
					fmt.Fprintf(os.Stderr, "httpClient.Get(%v) returned unexpected error: %v\n", containerURL, containerGetErr)
					os.Exit(1)
				}

				if http.StatusNoContent == containerGetResponse.StatusCode {
					// Nothing to do here
				} else if http.StatusOK == containerGetResponse.StatusCode {
					containerGetResponseBodyByteSlice, containerGetResponseBodyReadAllErr := ioutil.ReadAll(containerGetResponse.Body)
					if nil != containerGetResponseBodyReadAllErr {
						fmt.Fprintf(os.Stderr, "ioutil.ReadAll(httpClient.Get(%v).Body) returned unexpected error: %v\n", containerURL, containerGetResponseBodyReadAllErr)
						os.Exit(1)
					}
					containerGetResponseBodyString := utils.ByteSliceToString(containerGetResponseBodyByteSlice)
					containerGetResponseBodyStringSlice := strings.Split(containerGetResponseBodyString, "\n")

					for _, objectName := range containerGetResponseBodyStringSlice[:len(containerGetResponseBodyStringSlice)-1] {
						objectURL := containerURL + "/" + objectName

						objectDeleteRequest, objectDeleteRequestErr := http.NewRequest("DELETE", objectURL, nil)
						if nil != objectDeleteRequestErr {
							fmt.Fprintf(os.Stderr, "http.NewRequest(\"DELETE\", %v, nil) returned unexpected error: %v\n", objectURL, objectDeleteRequestErr)
							os.Exit(1)
						}
						objectDeleteResponse, objectDeleteResponseErr := httpClient.Do(objectDeleteRequest)
						if nil != objectDeleteResponseErr {
							fmt.Fprintf(os.Stderr, "httpClient.Do(DELETE %v) returned unexpected error: %v\n", objectURL, objectDeleteResponseErr)
							os.Exit(1)
						}
						if http.StatusNoContent != objectDeleteResponse.StatusCode {
							if verbose {
								fmt.Fprintf(os.Stderr, "httpClient.Do(DELETE %v) returned unexpected StatusCode: %v\n", objectURL, objectDeleteResponse.StatusCode)
							}
						}
						objectDeleteResponseBodyCloseErr := objectDeleteResponse.Body.Close()
						if nil != objectDeleteResponseBodyCloseErr {
							fmt.Fprintf(os.Stderr, "httpClient.Do(DELETE %v).Body.Close() returned unexpected error: %v\n", objectURL, objectDeleteResponseBodyCloseErr)
							os.Exit(1)
						}
					}
				} else {
					if verbose {
						fmt.Fprintf(os.Stderr, "httpClient.Get(%v) returned unexpected StatusCode: %v\n", containerURL, containerGetResponse.StatusCode)
					}
				}

				containerGetResponseBodyCloseErr := containerGetResponse.Body.Close()
				if nil != containerGetResponseBodyCloseErr {
					fmt.Fprintf(os.Stderr, "httpClient.Get(%v).Body.Close() returned unexpected error: %v\n", containerURL, containerGetResponseBodyCloseErr)
					os.Exit(1)
				}

				containerDeleteRequest, containerDeleteRequestErr := http.NewRequest("DELETE", containerURL, nil)
				if nil != containerDeleteRequestErr {
					fmt.Fprintf(os.Stderr, "http.NewRequest(\"DELETE\", %v, nil) returned unexpected error: %v\n", containerURL, containerDeleteRequestErr)
					os.Exit(1)
				}
				containerDeleteResponse, containerDeleteResponseErr := httpClient.Do(containerDeleteRequest)
				if nil != containerDeleteResponseErr {
					fmt.Fprintf(os.Stderr, "httpClient.Do(DELETE %v) returned unexpected error: %v\n", containerURL, containerDeleteResponseErr)
					os.Exit(1)
				}
				switch containerDeleteResponse.StatusCode {
				case http.StatusNoContent:
					// Nothing to do here
				case http.StatusConflict:
					if verbose {
						fmt.Fprintf(os.Stderr, "%v still not empty - re-run required\n", containerURL)
					}
					cumulativeFailures++
				default:
					if verbose {
						fmt.Fprintf(os.Stderr, "httpClient.Do(DELETE %v) returned unexpected StatusCode: %v\n", containerURL, containerDeleteResponse.StatusCode)
					}
				}
				containerDeleteResponseBodyCloseErr := containerDeleteResponse.Body.Close()
				if nil != containerDeleteResponseBodyCloseErr {
					fmt.Fprintf(os.Stderr, "httpClient.Do(DELETE %v).Body.Close() returned unexpected error: %v\n", containerURL, containerDeleteResponseBodyCloseErr)
					os.Exit(1)
				}

				replayLogFileName, confErr := confMap.FetchOptionValueString(volumeSectionName, "ReplayLogFileName")
				if nil == confErr {
					if "" != replayLogFileName {
						removeReplayLogFileErr := os.Remove(replayLogFileName)
						if nil != removeReplayLogFileErr {
							if !os.IsNotExist(removeReplayLogFileErr) {
								fmt.Fprintf(os.Stderr, "os.Remove(replayLogFileName == \"%v\") returned unexpected error: %v\n", replayLogFileName, removeReplayLogFileErr)
								os.Exit(1)
							}
						}
					}
				}
			}
		} else {
			if verbose {
				fmt.Fprintf(os.Stderr, "httpClient.Get(%v) returned unexpected StatusCode: %v\n", accountURL, accountGetResponse.StatusCode)
			}
		}

		accountGetResponseBodyCloseErr := accountGetResponse.Body.Close()
		if nil != accountGetResponseBodyCloseErr {
			fmt.Fprintf(os.Stderr, "httpClient.Get(%v).Body.Close() returned unexpected error: %v\n", accountURL, accountGetResponseBodyCloseErr)
			os.Exit(1)
		}
	}

	// Finally, remove Logging.LogFilePath (if any)
	logFilePath, confErr := confMap.FetchOptionValueString("Logging", "LogFilePath")
	if nil == confErr {
		if verbose {
			fmt.Fprintf(os.Stdout, "about to clear out %v\n", logFilePath)
		}

		osRemoveAllErr := os.RemoveAll(logFilePath)
		if nil != osRemoveAllErr {
			fmt.Fprintf(os.Stderr, "os.RemoveAll(logFilePath) failed: %v\n", osRemoveAllErr)
			os.Exit(cumulativeFailures + 1)
		}
	}

	os.Exit(cumulativeFailures)
}
