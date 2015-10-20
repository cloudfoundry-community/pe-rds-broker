package database

import (
	"github.com/pivotal-golang/lager"
)

type Provider interface {
	GetDatabase(engine string, logger lager.Logger) (Database, error)
}
