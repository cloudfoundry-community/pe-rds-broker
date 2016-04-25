package fakes

import (
	"github.com/alphagov/paas-rds-broker/awsrds"
)

type FakeDBCluster struct {
	DescribeCalled           bool
	DescribeID               string
	DescribeDBClusterDetails awsrds.DBClusterDetails
	DescribeError            error

	CreateCalled           bool
	CreateID               string
	CreateDBClusterDetails awsrds.DBClusterDetails
	CreateError            error

	ModifyCalled           bool
	ModifyID               string
	ModifyDBClusterDetails awsrds.DBClusterDetails
	ModifyApplyImmediately bool
	ModifyError            error

	DeleteCalled            bool
	DeleteID                string
	DeleteSkipFinalSnapshot bool
	DeleteError             error
}

func (f *FakeDBCluster) Describe(ID string) (awsrds.DBClusterDetails, error) {
	f.DescribeCalled = true
	f.DescribeID = ID

	return f.DescribeDBClusterDetails, f.DescribeError
}

func (f *FakeDBCluster) Create(ID string, dbClusterDetails awsrds.DBClusterDetails) error {
	f.CreateCalled = true
	f.CreateID = ID
	f.CreateDBClusterDetails = dbClusterDetails

	return f.CreateError
}

func (f *FakeDBCluster) Modify(ID string, dbClusterDetails awsrds.DBClusterDetails, applyImmediately bool) error {
	f.ModifyCalled = true
	f.ModifyID = ID
	f.ModifyDBClusterDetails = dbClusterDetails
	f.ModifyApplyImmediately = applyImmediately

	return f.ModifyError
}

func (f *FakeDBCluster) Delete(ID string, skipFinalSnapshot bool) error {
	f.DeleteCalled = true
	f.DeleteID = ID
	f.DeleteSkipFinalSnapshot = skipFinalSnapshot

	return f.DeleteError
}
