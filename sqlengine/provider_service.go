package sqlengine

import (
	"fmt"
	"strings"

	"code.cloudfoundry.org/lager"
)

// ProviderService object
type ProviderService struct {
	logger lager.Logger
}

// NewProviderService create new ProviderService
func NewProviderService(logger lager.Logger) *ProviderService {
	return &ProviderService{logger: logger}
}

// GetSQLEngine of ProviderService
func (p *ProviderService) GetSQLEngine(engine string) (SQLEngine, error) {
	switch strings.ToLower(engine) {
	case "aurora", "mariadb", "mysql":
		return NewMySQLEngine(p.logger), nil
	case "postgres", "postgresql":
		return NewPostgresEngine(p.logger), nil
	}

	return nil, fmt.Errorf("SQL Engine '%s' not supported", engine)
}
