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

type PFSagentConfig struct {
	confMap    conf.ConfMap
	configPath string
}

func (cfg PFSagentConfig) SetConfigPath(path string) {
	log.Printf("Updating config path from %v to %v", cfg.configPath, path)
	cfg.configPath = path
}

func (cfg PFSagentConfig) GetValue(key string) (val string, err error) {
	val, err = cfg.confMap.FetchOptionValueString("Agent", key)
	return
}

func (cfg PFSagentConfig) UpdateFromString(key, val string) (err error) {
	if len(key) == 0 {
		err = fmt.Errorf("key cannot be zero length")
		return
	}
	var prevVal string
	if prevVal, err = cfg.confMap.FetchOptionValueString("Agent", key); nil != err {
		return
	}
	log.Printf("Updating %v from %v to %v", key, prevVal, val)
	cfg.confMap.UpdateFromString(fmt.Sprintf("%v : %v", key, val))
	return
}

var (
	defaultMountPath = func() string {
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		return usr.HomeDir
	}() + "/pfsagentMount"
	// ConfigPath = defaultConfigPath
)

func (cfg PFSagentConfig) cloneFromTemplate() (configName string, err error) {
	tmplPath := fmt.Sprintf("%v/%v", cfg.configPath, configTmplFile)
	// fmt.Printf("tmplPath: %v\n", tmplPath)
	if _, err = os.Stat(tmplPath); err != nil {
		fmt.Println("Template file not found at", tmplPath, err)
		return
	}
	cfg.confMap, err = conf.MakeConfMapFromFile(tmplPath)
	if err != nil {
		log.Println("Failed loading template file", tmplPath, err)
		return
	}
	return
}

func (cfg PFSagentConfig) renameConfig(newName string) (err error) {
	if len(newName) == 0 {
		err = fmt.Errorf("no new name provided")
		return
	}
	var oldName string
	if oldName, err = cfg.confMap.FetchOptionValueString("Agent", "FUSEVolumeName"); nil != err {
		return
	}
	// oldName := cfg.confMap["Agent"]["FUSEVolumeName"][0]
	if newName == oldName {
		return
	}
	oldFilePath := fmt.Sprintf("%v/%v", cfg.configPath, oldName)
	if _, err = os.Stat(oldFilePath); err != nil {
		log.Printf("Config file not found at %v\n%v\n", oldFilePath, err)
		return
	}
	newFilePath := fmt.Sprintf("%v/%v", cfg.configPath, newName)
	if _, err = os.Stat(newFilePath); err == nil {
		log.Printf("%v already has a file: %v\n%v\n", newName, newFilePath, err)
		return
	}
	cfg.confMap.UpdateFromString(fmt.Sprintf("%v : %v", "Agent.FUSEVolumeName", newName))
	err = cfg.SaveCurrentConfig()
	if err == nil {
		os.Remove(oldFilePath)
	}
	return
}

func (cfg PFSagentConfig) LoadConfig(configName string) (err error) {
	if len(configName) == 0 {
		log.Printf("Cloning config from %v\n", configTmplFile)
		configName, err = cfg.cloneFromTemplate()
		return
	}

	configFilePath := fmt.Sprintf("%v/%v", cfg.configPath, configName)
	if configFilePath[len(configFilePath)-5:] != ".conf" {
		configFilePath = fmt.Sprintf("%v.conf", configFilePath)
	}
	log.Printf("Initializing config from %v\n", configFilePath)
	if _, err = os.Stat(configFilePath); err != nil {
		log.Println("Config file not found at", configFilePath, err)
		return
	}
	cfg.confMap, err = conf.MakeConfMapFromFile(configFilePath)
	if err != nil {
		log.Println("Failed loading config file", cfg.configPath, err)
		return
	}
	return
}

func (cfg PFSagentConfig) SaveCurrentConfig() (err error) {
	if cfg.confMap == nil {
		log.Println("Config is not initialized in the utility. did loadConfig() run?")
		err = errors.New("no config found")
		return
	}
	// configName := cfg.confMap["Agent"]["FUSEVolumeName"][0]
	configName, err := cfg.confMap.FetchOptionValueString("Agent", "FUSEVolumeName")
	configFilePath := fmt.Sprintf("%v/%v.conf", cfg.configPath, configName)
	fmt.Printf("saving config to %v\n", configFilePath)
	cfg.confMap.DumpConfMapToFile(configFilePath, os.ModePerm)

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
