package awsrds

import (
	"errors"
)

// DBCluster generic DBCluster interface
type DBCluster interface {
	Describe(ID string) (DBClusterDetails, error)
	Create(ID string, dbClusterDetails DBClusterDetails) error
	Modify(ID string, dbClusterDetails DBClusterDetails, applyImmediately bool) error
	Delete(ID string, skipFinalSnapshot bool) error
	List() ([]DBClusterDetails, error)
}

// DBClusterDetails struct holding information about the cluster
type DBClusterDetails struct {
	Identifier                  string
	Status                      string
	AllocatedStorage            int64
	AvailabilityZones           []string
	BackupRetentionPeriod       int64
	CharacterSetName            string
	DBClusterParameterGroupName string
	DBSubnetGroupName           string
	DatabaseName                string
	Endpoint                    string
	Engine                      string
	EngineVersion               string
	MasterUsername              string
	MasterUserPassword          string
	OptionGroupName             string
	Port                        int64
	PreferredBackupWindow       string
	PreferredMaintenanceWindow  string
	VpcSecurityGroupIds         []string
	Tags                        map[string]string
}

var (
	// ErrDBClusterDoesNotExist default error for not existing cluster
	ErrDBClusterDoesNotExist = errors.New("rds db cluster does not exist")
)
