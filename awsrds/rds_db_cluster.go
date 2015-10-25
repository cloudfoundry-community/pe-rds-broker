package awsrds

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/pivotal-golang/lager"
)

type RDSDBCluster struct {
	region string
	iamsvc *iam.IAM
	rdssvc *rds.RDS
	logger lager.Logger
}

func NewRDSDBCluster(
	region string,
	iamsvc *iam.IAM,
	rdssvc *rds.RDS,
	logger lager.Logger,
) *RDSDBCluster {
	return &RDSDBCluster{
		region: region,
		iamsvc: iamsvc,
		rdssvc: rdssvc,
		logger: logger.Session("db-cluster"),
	}
}

func (r *RDSDBCluster) Describe(ID string) (DBClusterDetails, error) {
	dbClusterDetails := DBClusterDetails{}

	describeDBClustersInput := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(ID),
	}

	r.logger.Debug("describe-db-clusters", lager.Data{"input": describeDBClustersInput})

	dbClusters, err := r.rdssvc.DescribeDBClusters(describeDBClustersInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return dbClusterDetails, ErrDBClusterDoesNotExist
				}
			}
			return dbClusterDetails, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return dbClusterDetails, err
	}

	for _, dbCluster := range dbClusters.DBClusters {
		if aws.StringValue(dbCluster.DBClusterIdentifier) == ID {
			r.logger.Debug("describe-db-clusters", lager.Data{"db-cluster": dbCluster})
			return r.buildDBCluster(dbCluster), nil
		}
	}

	return dbClusterDetails, ErrDBClusterDoesNotExist
}

func (r *RDSDBCluster) Create(ID string, dbClusterDetails DBClusterDetails) error {
	createDBClusterInput := r.buildCreateDBClusterInput(ID, dbClusterDetails)
	r.logger.Debug("create-db-cluster", lager.Data{"input": createDBClusterInput})

	createDBClusterOutput, err := r.rdssvc.CreateDBCluster(createDBClusterInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	r.logger.Debug("create-db-cluster", lager.Data{"output": createDBClusterOutput})

	return nil
}

func (r *RDSDBCluster) Modify(ID string, dbClusterDetails DBClusterDetails, applyImmediately bool) error {
	modifyDBClusterInput := r.buildModifyDBClusterInput(ID, dbClusterDetails, applyImmediately)
	r.logger.Debug("modify-db-cluster", lager.Data{"input": modifyDBClusterInput})

	modifyDBClusterOutput, err := r.rdssvc.ModifyDBCluster(modifyDBClusterInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return ErrDBClusterDoesNotExist
				}
			}
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	r.logger.Debug("modify-db-cluster", lager.Data{"output": modifyDBClusterOutput})

	if len(dbClusterDetails.Tags) > 0 {
		dbClusterARN, err := r.dbClusterARN(ID)
		if err != nil {
			return nil
		}

		tags := BuilRDSTags(dbClusterDetails.Tags)
		AddTagsToResource(dbClusterARN, tags, r.rdssvc, r.logger)
	}

	return nil
}

func (r *RDSDBCluster) Delete(ID string, skipFinalSnapshot bool) error {
	deleteDBClusterInput := r.buildDeleteDBClusterInput(ID, skipFinalSnapshot)
	r.logger.Debug("delete-db-cluster", lager.Data{"input": deleteDBClusterInput})

	deleteDBClusterOutput, err := r.rdssvc.DeleteDBCluster(deleteDBClusterInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return ErrDBClusterDoesNotExist
				}
			}
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	r.logger.Debug("delete-db-cluster", lager.Data{"output": deleteDBClusterOutput})

	return nil
}
func (r *RDSDBCluster) buildDBCluster(dbCluster *rds.DBCluster) DBClusterDetails {
	dbClusterDetails := DBClusterDetails{
		Identifier:       aws.StringValue(dbCluster.DBClusterIdentifier),
		Status:           aws.StringValue(dbCluster.Status),
		Engine:           aws.StringValue(dbCluster.Engine),
		EngineVersion:    aws.StringValue(dbCluster.EngineVersion),
		DatabaseName:     aws.StringValue(dbCluster.DatabaseName),
		MasterUsername:   aws.StringValue(dbCluster.MasterUsername),
		AllocatedStorage: aws.Int64Value(dbCluster.AllocatedStorage),
		Endpoint:         aws.StringValue(dbCluster.Endpoint),
		Port:             aws.Int64Value(dbCluster.Port),
	}

	return dbClusterDetails
}

func (r *RDSDBCluster) buildCreateDBClusterInput(ID string, dbClusterDetails DBClusterDetails) *rds.CreateDBClusterInput {
	createDBClusterInput := &rds.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(ID),
		Engine:              aws.String(dbClusterDetails.Engine),
	}

	if len(dbClusterDetails.AvailabilityZones) > 0 {
		createDBClusterInput.AvailabilityZones = aws.StringSlice(dbClusterDetails.AvailabilityZones)
	}

	if dbClusterDetails.BackupRetentionPeriod > 0 {
		createDBClusterInput.BackupRetentionPeriod = aws.Int64(dbClusterDetails.BackupRetentionPeriod)
	}

	if dbClusterDetails.CharacterSetName != "" {
		createDBClusterInput.CharacterSetName = aws.String(dbClusterDetails.CharacterSetName)
	}

	if dbClusterDetails.DatabaseName != "" {
		createDBClusterInput.DatabaseName = aws.String(dbClusterDetails.DatabaseName)
	}

	if dbClusterDetails.DBClusterParameterGroupName != "" {
		createDBClusterInput.DBClusterParameterGroupName = aws.String(dbClusterDetails.DBClusterParameterGroupName)
	}

	if dbClusterDetails.DBSubnetGroupName != "" {
		createDBClusterInput.DBSubnetGroupName = aws.String(dbClusterDetails.DBSubnetGroupName)
	}

	if dbClusterDetails.EngineVersion != "" {
		createDBClusterInput.EngineVersion = aws.String(dbClusterDetails.EngineVersion)
	}

	if dbClusterDetails.MasterUsername != "" {
		createDBClusterInput.MasterUsername = aws.String(dbClusterDetails.MasterUsername)
	}

	if dbClusterDetails.MasterUserPassword != "" {
		createDBClusterInput.MasterUserPassword = aws.String(dbClusterDetails.MasterUserPassword)
	}

	if dbClusterDetails.OptionGroupName != "" {
		createDBClusterInput.OptionGroupName = aws.String(dbClusterDetails.OptionGroupName)
	}

	if dbClusterDetails.Port > 0 {
		createDBClusterInput.Port = aws.Int64(dbClusterDetails.Port)
	}

	if dbClusterDetails.PreferredBackupWindow != "" {
		createDBClusterInput.PreferredBackupWindow = aws.String(dbClusterDetails.PreferredBackupWindow)
	}

	if dbClusterDetails.PreferredMaintenanceWindow != "" {
		createDBClusterInput.PreferredMaintenanceWindow = aws.String(dbClusterDetails.PreferredMaintenanceWindow)
	}

	if len(dbClusterDetails.VpcSecurityGroupIds) > 0 {
		createDBClusterInput.VpcSecurityGroupIds = aws.StringSlice(dbClusterDetails.VpcSecurityGroupIds)
	}

	if len(dbClusterDetails.Tags) > 0 {
		createDBClusterInput.Tags = BuilRDSTags(dbClusterDetails.Tags)
	}

	return createDBClusterInput
}

func (r *RDSDBCluster) buildModifyDBClusterInput(ID string, dbClusterDetails DBClusterDetails, applyImmediately bool) *rds.ModifyDBClusterInput {
	modifyDBClusterInput := &rds.ModifyDBClusterInput{
		DBClusterIdentifier: aws.String(ID),
		ApplyImmediately:    aws.Bool(applyImmediately),
	}

	if dbClusterDetails.BackupRetentionPeriod > 0 {
		modifyDBClusterInput.BackupRetentionPeriod = aws.Int64(dbClusterDetails.BackupRetentionPeriod)
	}

	if dbClusterDetails.DBClusterParameterGroupName != "" {
		modifyDBClusterInput.DBClusterParameterGroupName = aws.String(dbClusterDetails.DBClusterParameterGroupName)
	}

	if dbClusterDetails.MasterUserPassword != "" {
		modifyDBClusterInput.MasterUserPassword = aws.String(dbClusterDetails.MasterUserPassword)
	}

	if dbClusterDetails.OptionGroupName != "" {
		modifyDBClusterInput.OptionGroupName = aws.String(dbClusterDetails.OptionGroupName)
	}

	if dbClusterDetails.Port > 0 {
		modifyDBClusterInput.Port = aws.Int64(dbClusterDetails.Port)
	}

	if dbClusterDetails.PreferredBackupWindow != "" {
		modifyDBClusterInput.PreferredBackupWindow = aws.String(dbClusterDetails.PreferredBackupWindow)
	}

	if dbClusterDetails.PreferredMaintenanceWindow != "" {
		modifyDBClusterInput.PreferredMaintenanceWindow = aws.String(dbClusterDetails.PreferredMaintenanceWindow)
	}

	if len(dbClusterDetails.VpcSecurityGroupIds) > 0 {
		modifyDBClusterInput.VpcSecurityGroupIds = aws.StringSlice(dbClusterDetails.VpcSecurityGroupIds)
	}

	return modifyDBClusterInput
}

func (r *RDSDBCluster) buildDeleteDBClusterInput(ID string, skipFinalSnapshot bool) *rds.DeleteDBClusterInput {
	deleteDBClusterInput := &rds.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(ID),
		SkipFinalSnapshot:   aws.Bool(skipFinalSnapshot),
	}

	if !skipFinalSnapshot {
		deleteDBClusterInput.FinalDBSnapshotIdentifier = aws.String(r.dbSnapshotName(ID))
	}

	return deleteDBClusterInput
}

func (r *RDSDBCluster) dbSnapshotName(ID string) string {
	return fmt.Sprintf("rds-broker-%s-%s", ID, time.Now().Format("2006-01-02-15-04-05"))
}

func (r *RDSDBCluster) dbClusterARN(ID string) (string, error) {
	userAccount, err := UserAccount(r.iamsvc)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", r.region, userAccount, ID), nil
}
