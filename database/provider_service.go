package database

import (
	"fmt"
	"strings"

	"github.com/pivotal-golang/lager"
)

type ProviderService struct{}

func NewProviderService() *ProviderService {
	return &ProviderService{}
}

func (d *ProviderService) GetDatabase(engine string, logger lager.Logger) (Database, error) {
	switch strings.ToLower(engine) {
	case "aurora", "mariadb", "mysql":
		return NewMySQLDatabase(logger), nil
	case "postgres":
		return NewPostgresDatabase(logger), nil
	}

	return nil, fmt.Errorf("Database '%s' not supported", engine)
}
