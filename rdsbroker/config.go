package rdsbroker

import (
	"errors"
	"fmt"
)

// Config representation
type Config struct {
	Region                       string  `json:"region"`
	DBPrefix                     string  `json:"db_prefix"`
	AllowUserProvisionParameters bool    `json:"allow_user_provision_parameters"`
	AllowUserUpdateParameters    bool    `json:"allow_user_update_parameters"`
	AllowUserBindParameters      bool    `json:"allow_user_bind_parameters"`
	MasterPasswordSalt           string  `json:"master_password_salt,omitempty"`
	ServiceBrokerID              string  `json:"service_broker_id,omitempty"`
	Catalog                      Catalog `json:"catalog"`
}

// Validate config
func (c Config) Validate() error {
	if c.Region == "" {
		return errors.New("Must provide a non-empty Region")
	}

	if c.DBPrefix == "" {
		return errors.New("Must provide a non-empty DBPrefix")
	}

	if err := c.Catalog.Validate(); err != nil {
		return fmt.Errorf("Validating Catalog configuration: %s", err)
	}

	return nil
}
