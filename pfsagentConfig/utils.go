package pfsagentConfig

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"

	"github.com/swiftstack/ProxyFS/conf"
)

const (
	defaultConfigPath string = "/etc/pfsagent"
	defaultLogPath    string = "/var/log/pfsagent"
	configTmplFile    string = "pfsagent.tmpl"
)

var (
	confMap          conf.ConfMap
	defaultMountPath = func() string {
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		return usr.HomeDir
	}() + "/pfsagentMount"
	ConfigPath = defaultConfigPath
)

func cloneFromTemplate() (configName string, err error) {
	tmplPath := fmt.Sprintf("%v/%v", ConfigPath, configTmplFile)
	// fmt.Printf("tmplPath: %v\n", tmplPath)
	if _, err = os.Stat(tmplPath); err != nil {
		fmt.Println("Template file not found at", tmplPath, err)
		return
	}
	confMap, err = conf.MakeConfMapFromFile(tmplPath)
	if err != nil {
		log.Println("Failed loading template file", tmplPath, err)
		return
	}
	return
}

func renameConfig(newName string) (err error) {
	if len(newName) == 0 {
		err = fmt.Errorf("no new name provided")
		return
	}
	oldName := confMap["Agent"]["FUSEVolumeName"][0]
	if newName == oldName {
		return
	}
	oldFilePath := fmt.Sprintf("%v/%v", ConfigPath, oldName)
	if _, err = os.Stat(oldFilePath); err != nil {
		log.Printf("Config file not found at %v\n%v\n", oldFilePath, err)
		return
	}
	newFilePath := fmt.Sprintf("%v/%v", ConfigPath, newName)
	if _, err = os.Stat(newFilePath); err == nil {
		log.Printf("%v already has a file: %v\n%v\n", newName, newFilePath, err)
		return
	}
	confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.FUSEVolumeName", newName))
	err = SaveCurrentConfig()
	if err == nil {
		os.Remove(oldFilePath)
	}
	return
}

func LoadConfig(configName string) (err error) {
	if len(configName) == 0 {
		log.Printf("Cloning config from %v\n", configTmplFile)
		configName, err = cloneFromTemplate()
		return
	}

	log.Printf("Initializing config from %v/%v\n", ConfigPath, configName)
	configFilePath := fmt.Sprintf("%v/%v", ConfigPath, configName)
	if _, err = os.Stat(configFilePath); err != nil {
		log.Println("Config file not found at", configFilePath, err)
		return
	}
	confMap, err = conf.MakeConfMapFromFile(configFilePath)
	// iniContent, loadErr := ini.Load(ConfigPath)
	if err != nil {
		log.Println("Failed loading config file", ConfigPath, err)
		return
	}
	return
}

func SaveCurrentConfig() (err error) {
	if confMap == nil {
		log.Println("Config is not initialized in the utility. did loadConfig() run?")
		err = errors.New("no config found")
		return
	}
	configName := confMap["Agent"]["FUSEVolumeName"][0]
	configFilePath := fmt.Sprintf("%v/%v.conf", ConfigPath, configName)
	fmt.Printf("saving config to %v\n", configFilePath)
	confMap.DumpConfMapToFile(configFilePath, os.ModePerm)

	return nil
}

func getUserInput() (response string, err error) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if err = scanner.Err(); err != nil {
			log.Println("Error reading standard input:", err)
			return
		}
		response = scanner.Text()
		return
	}
	err = fmt.Errorf("Error retrieving user input")
	return
}

func getValueFromUser(title string, text string, currentValue string) (response string, err error) {
	fmt.Printf("** Changing %v **", title)
	if len(text) > 0 {
		fmt.Printf("\n\t%v", text)
	}
	fmt.Printf("\n\nCurrent Value: %v\nNew Value: ", currentValue)
	response, err = getUserInput()
	if err != nil {
		log.Println("Error retrieving user input", err)
	}
	return
}
