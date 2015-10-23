package awsrds

import (
	"errors"
)

type DBInstance interface {
	Describe(ID string) (DBInstanceDetails, error)
	Create(ID string, dbInstanceDetails DBInstanceDetails) error
	Modify(ID string, dbInstanceDetails DBInstanceDetails, applyImmediately bool) error
	Delete(ID string, skipFinalSnapshot bool) error
}

type DBInstanceDetails struct {
	Identifier                 string
	Status                     string
	DBInstanceClass            string
	Engine                     string
	EngineVersion              string
	Address                    string
	AllocatedStorage           int64
	AutoMinorVersionUpgrade    bool
	AvailabilityZone           string
	BackupRetentionPeriod      int64
	CharacterSetName           string
	CopyTagsToSnapshot         bool
	DBName                     string
	DBParameterGroupName       string
	DBSecurityGroups           []string
	DBSubnetGroupName          string
	Iops                       int64
	KmsKeyID                   string
	LicenseModel               string
	MasterUsername             string
	MasterUserPassword         string
	MultiAZ                    bool
	OptionGroupName            string
	PendingModifications       bool
	Port                       int64
	PreferredBackupWindow      string
	PreferredMaintenanceWindow string
	PubliclyAccessible         bool
	StorageEncrypted           bool
	StorageType                string
	Tags                       map[string]string
	VpcSecurityGroupIds        []string
}

var (
	ErrDBInstanceDoesNotExist = errors.New("rds db instance does not exist")
)
