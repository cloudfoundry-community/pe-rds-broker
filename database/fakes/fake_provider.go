package fakes

import (
	"github.com/cf-platform-eng/rds-broker/database"
	"github.com/pivotal-golang/lager"
)

type FakeProvider struct {
	GetDatabaseCalled   bool
	GetDatabaseEngine   string
	GetDatabaseDatabase database.Database
	GetDatabaseError    error
}

func (f *FakeProvider) GetDatabase(engine string, logger lager.Logger) (database.Database, error) {
	f.GetDatabaseCalled = true
	f.GetDatabaseEngine = engine

	return f.GetDatabaseDatabase, f.GetDatabaseError
}
