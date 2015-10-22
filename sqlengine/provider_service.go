package sqlengine

import (
	"fmt"
	"strings"

	"github.com/pivotal-golang/lager"
)

type ProviderService struct {
	logger lager.Logger
}

func NewProviderService(logger lager.Logger) *ProviderService {
	return &ProviderService{logger: logger}
}

func (p *ProviderService) GetSQLEngine(engine string) (SQLEngine, error) {
	switch strings.ToLower(engine) {
	case "aurora", "mariadb", "mysql":
		return NewMySQLEngine(p.logger), nil
	case "postgres", "postgresql":
		return NewPostgresEngine(p.logger), nil
	}

	return nil, fmt.Errorf("SQL Engine '%s' not supported", engine)
}
