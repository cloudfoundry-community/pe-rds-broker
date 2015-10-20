package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/frodenas/brokerapi"
	"github.com/pivotal-golang/lager"

	"github.com/cf-platform-eng/rds-broker/database"
	"github.com/cf-platform-eng/rds-broker/rdsbroker"
)

var (
	configFilePath string
	port           string

	logLevels = map[string]lager.LogLevel{
		"DEBUG": lager.DEBUG,
		"INFO":  lager.INFO,
		"ERROR": lager.ERROR,
		"FATAL": lager.FATAL,
	}
)

func init() {
	flag.StringVar(&configFilePath, "config", "", "Location of the config file")
	flag.StringVar(&port, "port", "3000", "Listen port")
}

func buildLogger(logLevel string) lager.Logger {
	laggerLogLevel, ok := logLevels[strings.ToUpper(logLevel)]
	if !ok {
		log.Fatal("Invalid log level: ", logLevel)
	}

	logger := lager.NewLogger("rds-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, laggerLogLevel))

	return logger
}

func main() {
	flag.Parse()

	config, err := LoadConfig(configFilePath)
	if err != nil {
		log.Fatalf("Error loading config file: %s", err)
	}

	awsConfig := aws.NewConfig().WithRegion(config.RDSConfig.Region)
	iamsvc := iam.New(awsConfig)
	rdssvc := rds.New(awsConfig)
	dbProvider := database.NewProviderService()
	logger := buildLogger(config.LogLevel)
	serviceBroker := rdsbroker.New(config.RDSConfig, iamsvc, rdssvc, dbProvider, logger)

	credentials := brokerapi.BrokerCredentials{
		Username: config.Username,
		Password: config.Password,
	}

	brokerAPI := brokerapi.New(serviceBroker, logger, credentials)
	http.Handle("/", brokerAPI)

	fmt.Println("RDS Service Broker started on port " + port + "...")
	http.ListenAndServe(":"+port, nil)
}
