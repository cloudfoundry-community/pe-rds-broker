package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/joek/brokerapi"

	"github.com/cloudfoundry-community/pe-rds-broker/awsrds"
	"github.com/cloudfoundry-community/pe-rds-broker/rdsbroker"
	"github.com/cloudfoundry-community/pe-rds-broker/sqlengine"
)

var (
	configFilePath string
	port           string
	setPasswords   bool

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
	flag.BoolVar(&setPasswords, "setPasswords", false, "Will update all master passwords. Use this if you like to change the master generation method or update the salt.")
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

	logger := buildLogger(config.LogLevel)

	awsConfig := aws.NewConfig().WithRegion(config.RDSConfig.Region)
	awsSession := session.New(awsConfig)

	iamsvc := iam.New(awsSession)
	rdssvc := rds.New(awsSession)
	dbInstance := awsrds.NewRDSDBInstance(config.RDSConfig.Region, iamsvc, rdssvc, logger)
	dbCluster := awsrds.NewRDSDBCluster(config.RDSConfig.Region, iamsvc, rdssvc, logger)

	sqlProvider := sqlengine.NewProviderService(logger)

	serviceBroker := rdsbroker.New(config.RDSConfig, dbInstance, dbCluster, sqlProvider, logger)

	if setPasswords {
		log.Println("SetPasswords started")
		err := rdsbroker.UpdatePasswords(serviceBroker)
		if err != nil {
			log.Fatalf("Error setting passwords: %s", err)
		}

	} else {
		credentials := brokerapi.BrokerCredentials{
			Username: config.Username,
			Password: config.Password,
		}

		brokerAPI := brokerapi.New(serviceBroker, logger, credentials)
		http.Handle("/", brokerAPI)

		fmt.Println("RDS Service Broker started on port " + port + "...")
		http.ListenAndServe(":"+port, nil)
	}
}
