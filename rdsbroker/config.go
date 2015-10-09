package rdsbroker

import (
	"errors"
	"fmt"
)

type Config struct {
	Region                       string  `json:"region"`
	MaxDBInstances               int     `json:"max_db_instances"`
	AllowUserProvisionParameters bool    `json:"allow_user_provision_parameters"`
	AllowUserUpdateParameters    bool    `json:"allow_user_update_parameters"`
	AllowUserBindParameters      bool    `json:"allow_user_bind_parameters"`
	Catalog                      Catalog `json:"catalog"`
}

func (c Config) Validate() error {
	if c.Region == "" {
		return errors.New("Must provide a non-empty Region")
	}

	if err := c.Catalog.Validate(); err != nil {
		return fmt.Errorf("Validating Catalog configuration: %s", err)
	}

	return nil
}
