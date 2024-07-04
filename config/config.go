package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type ConfigEnvironment struct {
	DefaultEnv      string
	EnvVariableName string
}

func init() {
	fmt.Println("from config init")
	initializeConfig()
}

func initializeConfig() {

	var configBasePath string = "./config/"
	env := getEnvironment()

	// Get config file
	var pathForConfig = configBasePath + "config-" + env
	_, fileErr := os.Stat(pathForConfig + ".json")

	if fileErr != nil {
		fmt.Println("Config file not found at: " + pathForConfig + ". trying to fetch default config.json")
		fmt.Println(fileErr)
		pathForConfig = configBasePath + "config"
	}

	viper.SetConfigName(pathForConfig) // name of config file (without extension)
	viper.SetConfigType("json")        // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		fmt.Println("Error in reading configuration through viper")
		log.Fatal(err)
	}
}

func getEnvironment() string {

	fmt.Println("config:reading environment")

	var environment string

	envData, envErr := os.ReadFile("./config/env.json")

	if envErr != nil {
		fmt.Println("Error in reading environment json file")
		log.Fatal(envErr)
	}

	var confEnv ConfigEnvironment
	jsonErr := json.Unmarshal(envData, &confEnv)

	if jsonErr != nil {
		fmt.Println("Error in parsing environment json file")
		log.Fatal(jsonErr)
	}

	if strings.TrimSpace(confEnv.EnvVariableName) != "" {
		fmt.Println("config:reading environment details from environment variable")
		environment = os.Getenv(confEnv.EnvVariableName)
	}

	if strings.TrimSpace(environment) == "" {
		fmt.Println("config:reading environment details from env default")
		environment = strings.TrimSpace(confEnv.DefaultEnv)
	}

	if strings.TrimSpace(environment) == "" {
		environment = "production"
		fmt.Println("config:environment configuration did not found, returning: " + environment)
	}

	fmt.Println(environment)
	return environment
}

// Methods to get value based on key - for other packages

func GetInterface(key string) interface{} {
	return viper.Get(key)
}

func GetString(key string) string {
	return viper.GetString(key)
}

func GetBool(key string) bool {
	return viper.GetBool(key)
}

func GetDuration(key string) time.Duration {
	return viper.GetDuration(key)
}

func GetFloat64(key string) float64 {
	return viper.GetFloat64(key)
}

func GetInt(key string) int {
	return viper.GetInt(key)
}

func GetInt32(key string) int32 {
	return viper.GetInt32(key)
}

func GetInt64(key string) int64 {
	return viper.GetInt64(key)
}

func GetIntSlice(key string) []int {
	return viper.GetIntSlice(key)
}

func GetStringSlice(key string) []string {
	return viper.GetStringSlice(key)
}

func GetTime(key string) time.Time {
	return viper.GetTime(key)
}

func GetUint(key string) uint {
	return viper.GetUint(key)
}

func GetUint32(key string) uint32 {
	return viper.GetUint32(key)
}

func GetUint64(key string) uint64 {
	return viper.GetUint64(key)
}
