package rdsbroker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL Driver
	_ "github.com/lib/pq"              // PostgreSQL Driver

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/frodenas/brokerapi"
	"github.com/mitchellh/mapstructure"
	"github.com/pivotal-golang/lager"

	"github.com/cf-platform-eng/rds-broker/utils"
)

const defaultUsernameLength = 16
const defaultPasswordLength = 32

const instanceIDLogKey = "instance-id"
const bindingIDLogKey = "binding-id"
const detailsLogKey = "details"
const acceptsIncompleteLogKey = "acceptsIncomplete"

var rdsStatus2State = map[string]string{
	"available":                    "succeeded",
	"backing-up":                   "in progress",
	"creating":                     "in progress",
	"deleting":                     "in progress",
	"maintenance":                  "in progress",
	"modifying":                    "in progress",
	"rebooting":                    "in progress",
	"renaming":                     "in progress",
	"resetting-master-credentials": "in progress",
	"upgrading":                    "in progress",
}

type CredentialsHash struct {
	Host     string `json:"host"`
	Port     int64  `json:"port"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Password string `json:"password"`
	URI      string `json:"uri"`
	JDBCURI  string `json:"jdbcUrl"`
}

type RDSBroker struct {
	region                       string
	dbPrefix                     string
	maxDBInstances               int
	allowUserProvisionParameters bool
	allowUserUpdateParameters    bool
	allowUserBindParameters      bool
	catalog                      Catalog
	logger                       lager.Logger
	iamsvc                       *iam.IAM
	rdssvc                       *rds.RDS
}

func New(
	config Config,
	logger lager.Logger,
) *RDSBroker {
	awsConfig := aws.NewConfig().WithRegion(config.Region)
	return &RDSBroker{
		region:                       config.Region,
		dbPrefix:                     config.DBPrefix,
		maxDBInstances:               config.MaxDBInstances,
		allowUserProvisionParameters: config.AllowUserProvisionParameters,
		allowUserUpdateParameters:    config.AllowUserUpdateParameters,
		allowUserBindParameters:      config.AllowUserBindParameters,
		catalog:                      config.Catalog,
		logger:                       logger,
		iamsvc:                       iam.New(awsConfig),
		rdssvc:                       rds.New(awsConfig),
	}
}

func (b *RDSBroker) Services() brokerapi.CatalogResponse {
	catalogResponse := brokerapi.CatalogResponse{}

	logger := b.logger.Session("broker.services")

	brokerCatalog, err := json.Marshal(b.catalog)
	if err != nil {
		logger.Error("marshal-error", err)
		return catalogResponse
	}

	apiCatalog := brokerapi.Catalog{}
	if err = json.Unmarshal(brokerCatalog, &apiCatalog); err != nil {
		logger.Error("unmarshal-error", err)
		return catalogResponse
	}

	catalogResponse.Services = apiCatalog.Services

	return catalogResponse
}

func (b *RDSBroker) Provision(instanceID string, details brokerapi.ProvisionDetails, acceptsIncomplete bool) (brokerapi.ProvisioningResponse, bool, error) {
	logger := b.logger.Session("broker.provision", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		acceptsIncompleteLogKey: acceptsIncomplete,
	})

	provisioningResponse := brokerapi.ProvisioningResponse{}

	if !acceptsIncomplete {
		return provisioningResponse, false, brokerapi.ErrAsyncRequired
	}

	provisionParameters := ProvisionParameters{}
	if b.allowUserProvisionParameters {
		if err := mapstructure.Decode(details.Parameters, &provisionParameters); err != nil {
			return provisioningResponse, false, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return provisioningResponse, false, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	createDBInstanceInput := b.buildCreateDBInstanceInput(instanceID, details, provisionParameters, servicePlan)
	logger.Debug("create-db-instance", lager.Data{"input": createDBInstanceInput})

	createDBInstanceInput.MasterUsername = aws.String(b.masterUsername())
	createDBInstanceInput.MasterUserPassword = aws.String(b.masterPassword(instanceID))

	createDBInstanceOutput, err := b.rdssvc.CreateDBInstance(createDBInstanceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return provisioningResponse, false, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return provisioningResponse, false, err
	}

	logger.Debug("create-db-instance", lager.Data{"output": createDBInstanceOutput})

	return provisioningResponse, true, nil
}

func (b *RDSBroker) Update(instanceID string, details brokerapi.UpdateDetails, acceptsIncomplete bool) (bool, error) {
	logger := b.logger.Session("broker.update", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		acceptsIncompleteLogKey: acceptsIncomplete,
	})

	if !acceptsIncomplete {
		return false, brokerapi.ErrAsyncRequired
	}

	updateParameters := UpdateParameters{}
	if b.allowUserUpdateParameters {
		if err := mapstructure.Decode(details.Parameters, &updateParameters); err != nil {
			return false, err
		}
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return false, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if !service.PlanUpdateable {
		return false, brokerapi.ErrInstanceNotUpdateable
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	dbInstance, err := b.describeDBInstance(instanceID, logger)
	if err != nil {
		return false, err
	}

	modifyDBInstanceInput, err := b.buildModifyDBInstanceInput(instanceID, details, updateParameters, servicePlan, dbInstance)
	if err != nil {
		return false, err
	}

	logger.Debug("modify-db-instance", lager.Data{"input": modifyDBInstanceInput})

	modifyDBInstanceOutput, err := b.rdssvc.ModifyDBInstance(modifyDBInstanceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return false, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return false, err
	}

	logger.Debug("modify-db-instance", lager.Data{"output": modifyDBInstanceOutput})

	tags := b.buildInstanceTags("Updated", details.ServiceID, details.PlanID, "", "")
	b.addTagsToResource(instanceID, tags, logger)

	return true, nil
}

func (b *RDSBroker) Deprovision(instanceID string, details brokerapi.DeprovisionDetails, acceptsIncomplete bool) (bool, error) {
	logger := b.logger.Session("broker.deprovision", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		acceptsIncompleteLogKey: acceptsIncomplete,
	})

	if !acceptsIncomplete {
		return false, brokerapi.ErrAsyncRequired
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	deleteDBInstanceInput := b.buildDeleteDBInstanceInput(instanceID, servicePlan)
	logger.Debug("delete-db-instance", lager.Data{"input": deleteDBInstanceInput})

	deleteDBInstanceOutput, err := b.rdssvc.DeleteDBInstance(deleteDBInstanceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return false, brokerapi.ErrInstanceDoesNotExist
				}
			}
			return false, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return false, err
	}

	logger.Debug("delete-db-instance", lager.Data{"output": deleteDBInstanceOutput})

	return true, nil
}

func (b *RDSBroker) Bind(instanceID, bindingID string, details brokerapi.BindDetails) (brokerapi.BindingResponse, error) {
	logger := b.logger.Session("broker.bind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	bindingResponse := brokerapi.BindingResponse{}

	bindParameters := BindParameters{}
	if b.allowUserBindParameters {
		if err := mapstructure.Decode(details.Parameters, &bindParameters); err != nil {
			return bindingResponse, err
		}
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return bindingResponse, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if !service.Bindable {
		return bindingResponse, brokerapi.ErrInstanceNotBindable
	}

	_, ok = b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return bindingResponse, fmt.Errorf("Plan '%s' not found", details.PlanID)
	}

	dbInstance, err := b.describeDBInstance(instanceID, logger)
	if err != nil {
		return bindingResponse, err
	}

	connectionString, err := b.connectionString(*dbInstance.Engine, *dbInstance.Endpoint.Address, *dbInstance.Endpoint.Port, *dbInstance.DBName, *dbInstance.MasterUsername, b.masterPassword(instanceID))
	if err != nil {
		return bindingResponse, err
	}

	logger.Debug("sql-open", lager.Data{"connection-string": connectionString})

	db, err := sql.Open(*dbInstance.Engine, connectionString)
	if err != nil {
		return bindingResponse, err
	}
	defer db.Close()

	dbUsername := b.dbUsername(bindingID)
	dbPassword := b.dbPassword()
	dbName := *dbInstance.DBName

	if bindParameters.DBName != "" {
		dbName = bindParameters.DBName
		if err = b.createDB(db, *dbInstance.Engine, dbName, logger); err != nil {
			return bindingResponse, err
		}
	}

	if err = b.createUser(db, *dbInstance.Engine, dbUsername, dbPassword, logger); err != nil {
		return bindingResponse, err
	}

	if err = b.grantPrivileges(db, *dbInstance.Engine, dbName, dbUsername, logger); err != nil {
		return bindingResponse, err
	}

	bindingResponse.Credentials = &CredentialsHash{
		Host:     *dbInstance.Endpoint.Address,
		Port:     *dbInstance.Endpoint.Port,
		Name:     dbName,
		Username: dbUsername,
		Password: dbPassword,
		URI:      fmt.Sprintf("%s://%s:%s@%s:%d/%s?reconnect=true", *dbInstance.Engine, dbUsername, dbPassword, *dbInstance.Endpoint.Address, *dbInstance.Endpoint.Port, dbName),
		JDBCURI:  fmt.Sprintf("jdbc:%s://%s:%d/%s?user=%s&password=%s", *dbInstance.Engine, *dbInstance.Endpoint.Address, *dbInstance.Endpoint.Port, dbName, dbUsername, dbPassword),
	}

	return bindingResponse, nil
}

func (b *RDSBroker) Unbind(instanceID, bindingID string, details brokerapi.UnbindDetails) error {
	logger := b.logger.Session("broker.unbind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	dbInstance, err := b.describeDBInstance(instanceID, logger)
	if err != nil {
		return err
	}

	connectionString, err := b.connectionString(*dbInstance.Engine, *dbInstance.Endpoint.Address, *dbInstance.Endpoint.Port, *dbInstance.DBName, *dbInstance.MasterUsername, b.masterPassword(instanceID))
	if err != nil {
		return err
	}

	logger.Debug("sql-open", lager.Data{"connection-string": connectionString})

	db, err := sql.Open(*dbInstance.Engine, connectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	privileges, err := b.dbPrivileges(db, *dbInstance.Engine, logger)
	if err != nil {
		return err
	}

	var userDB string
	dbUsername := b.dbUsername(bindingID)
	for dbName, userNames := range privileges {
		for _, userName := range userNames {
			if userName == dbUsername {
				userDB = dbName
				break
			}
		}
	}

	if userDB != "" {
		if err = b.revokePrivileges(db, *dbInstance.Engine, userDB, dbUsername, logger); err != nil {
			return err
		}

		if userDB != *dbInstance.DBName {
			users := privileges[userDB]
			if len(users) == 1 {
				if err = b.dropDB(db, *dbInstance.Engine, userDB, logger); err != nil {
					return err
				}
			}
		}
	}

	if err = b.dropUser(db, *dbInstance.Engine, dbUsername, logger); err != nil {
		return err
	}

	return nil
}

func (b *RDSBroker) LastOperation(instanceID string) (brokerapi.LastOperationResponse, error) {
	logger := b.logger.Session("broker.last-operation", lager.Data{
		instanceIDLogKey: instanceID,
	})

	lastOperationResponse := brokerapi.LastOperationResponse{State: "failed"}

	dbInstance, err := b.describeDBInstance(instanceID, logger)
	if err != nil {
		return lastOperationResponse, err
	}

	if state, ok := rdsStatus2State[*dbInstance.DBInstanceStatus]; ok {
		lastOperationResponse.State = state
	}

	return lastOperationResponse, nil
}

func (b *RDSBroker) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s_%s", b.dbPrefix, strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSBroker) masterUsername() string {
	return utils.RandomAlphaNum(defaultUsernameLength)
}

func (b *RDSBroker) masterPassword(instanceID string) string {
	return utils.GetMD5B64(instanceID, defaultPasswordLength)
}

func (b *RDSBroker) dbName(instanceID string) string {
	return fmt.Sprintf("%s_%s", b.dbPrefix, strings.Replace(instanceID, "-", "_", -1))
}

func (b *RDSBroker) dbSnapshotName(instanceID string) string {
	return fmt.Sprintf("rds-broker-%s-%s", strings.Replace(b.dbName(instanceID), "_", "-", -1), time.Now().Format("2006-01-02-15-04-05"))
}

func (b *RDSBroker) dbUsername(bindingID string) string {
	return utils.GetMD5B64(bindingID, defaultUsernameLength)
}

func (b *RDSBroker) dbPassword() string {
	return utils.RandomAlphaNum(defaultPasswordLength)
}

func (b *RDSBroker) instanceARN(instanceID string, logger lager.Logger) (string, error) {
	userARN, err := b.userARN(logger)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", b.region, userARN, instanceID), nil
}

func (b *RDSBroker) userARN(logger lager.Logger) (string, error) {
	getUserInput := &iam.GetUserInput{}
	getUserOutput, err := b.iamsvc.GetUser(getUserInput)
	if err != nil {
		logger.Error("aws-iam-error", err)
		return "", err
	}

	userARN := strings.Split(*getUserOutput.User.Arn, ":")

	return userARN[4], nil
}

func (b *RDSBroker) describeDBInstance(instanceID string, logger lager.Logger) (*rds.DBInstance, error) {
	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
	}

	logger.Debug("describe-db-instances", lager.Data{"input": describeDBInstancesInput})

	dbInstances, err := b.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return nil, brokerapi.ErrInstanceDoesNotExist
				}
			}
			return nil, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return nil, err
	}

	logger.Debug("describe-db-instances", lager.Data{"db-instances": dbInstances})

	for _, dbInstance := range dbInstances.DBInstances {
		if b.dbInstanceIdentifier(instanceID) == *dbInstance.DBInstanceIdentifier {
			return dbInstance, nil
		}
	}

	return nil, brokerapi.ErrInstanceDoesNotExist
}

func (b *RDSBroker) addTagsToResource(instanceID string, tags []*rds.Tag, logger lager.Logger) error {
	instanceARN, err := b.instanceARN(instanceID, logger)
	if err != nil {
		return err
	}

	addTagsToResourceInput := &rds.AddTagsToResourceInput{
		ResourceName: aws.String(instanceARN),
		Tags:         tags,
	}

	logger.Debug("add-tags-to-resource", lager.Data{"input": addTagsToResourceInput})

	addTagsToResourceOutput, err := b.rdssvc.AddTagsToResource(addTagsToResourceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return brokerapi.ErrInstanceDoesNotExist
				}
			}
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	logger.Debug("add-tags-to-resource", lager.Data{"output": addTagsToResourceOutput})

	return nil
}

func (b *RDSBroker) buildInstanceTags(action, serviceID, planID, organizationID, spaceID string) []*rds.Tag {
	var tags []*rds.Tag

	tags = append(tags, &rds.Tag{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")})

	tags = append(tags, &rds.Tag{Key: aws.String(action + " by"), Value: aws.String("RDS Service Broker")})

	tags = append(tags, &rds.Tag{Key: aws.String(action + " at"), Value: aws.String(time.Now().Format(time.RFC822Z))})

	if serviceID != "" {
		tags = append(tags, &rds.Tag{Key: aws.String("Service ID"), Value: aws.String(serviceID)})
	}

	if planID != "" {
		tags = append(tags, &rds.Tag{Key: aws.String("Plan ID"), Value: aws.String(planID)})
	}

	if organizationID != "" {
		tags = append(tags, &rds.Tag{Key: aws.String("Organization ID"), Value: aws.String(organizationID)})
	}

	if spaceID != "" {
		tags = append(tags, &rds.Tag{Key: aws.String("Space ID"), Value: aws.String(spaceID)})
	}

	return tags
}

func (b *RDSBroker) buildCreateDBInstanceInput(
	instanceID string,
	details brokerapi.ProvisionDetails,
	provisionParameters ProvisionParameters,
	servicePlan ServicePlan,
) *rds.CreateDBInstanceInput {
	createDBInstanceInput := &rds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
		DBInstanceClass:      aws.String(servicePlan.RDSProperties.DBInstanceClass),
		Engine:               aws.String(servicePlan.RDSProperties.Engine),
		EngineVersion:        aws.String(servicePlan.RDSProperties.EngineVersion),
		AllocatedStorage:     aws.Int64(servicePlan.RDSProperties.AllocatedStorage),
		DBName:               aws.String(b.dbName(instanceID)),
	}

	createDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(servicePlan.RDSProperties.AutoMinorVersionUpgrade)

	if servicePlan.RDSProperties.AvailabilityZone != "" {
		createDBInstanceInput.AvailabilityZone = aws.String(servicePlan.RDSProperties.AvailabilityZone)
	}

	if servicePlan.RDSProperties.BackupRetentionPeriod > 0 {
		createDBInstanceInput.BackupRetentionPeriod = aws.Int64(servicePlan.RDSProperties.BackupRetentionPeriod)
	}

	if provisionParameters.BackupRetentionPeriod > 0 {
		createDBInstanceInput.BackupRetentionPeriod = aws.Int64(provisionParameters.BackupRetentionPeriod)
	}

	if servicePlan.RDSProperties.CharacterSetName != "" {
		createDBInstanceInput.CharacterSetName = aws.String(servicePlan.RDSProperties.CharacterSetName)
	}

	if provisionParameters.CharacterSetName != "" {
		createDBInstanceInput.CharacterSetName = aws.String(provisionParameters.CharacterSetName)
	}

	if servicePlan.RDSProperties.DBName != "" {
		createDBInstanceInput.DBName = aws.String(servicePlan.RDSProperties.DBName)
	}

	if provisionParameters.DBName != "" {
		createDBInstanceInput.DBName = aws.String(provisionParameters.DBName)
	}

	if servicePlan.RDSProperties.DBParameterGroupName != "" {
		createDBInstanceInput.DBParameterGroupName = aws.String(servicePlan.RDSProperties.DBParameterGroupName)
	}

	if len(servicePlan.RDSProperties.DBSecurityGroups) > 0 {
		createDBInstanceInput.DBSecurityGroups = aws.StringSlice(servicePlan.RDSProperties.DBSecurityGroups)
	}

	if servicePlan.RDSProperties.DBSubnetGroupName != "" {
		createDBInstanceInput.DBSubnetGroupName = aws.String(servicePlan.RDSProperties.DBSubnetGroupName)
	}

	if servicePlan.RDSProperties.LicenseModel != "" {
		createDBInstanceInput.LicenseModel = aws.String(servicePlan.RDSProperties.LicenseModel)
	}

	createDBInstanceInput.MultiAZ = aws.Bool(servicePlan.RDSProperties.MultiAZ)

	if servicePlan.RDSProperties.OptionGroupName != "" {
		createDBInstanceInput.OptionGroupName = aws.String(servicePlan.RDSProperties.OptionGroupName)
	}

	if servicePlan.RDSProperties.Port > 0 {
		createDBInstanceInput.Port = aws.Int64(servicePlan.RDSProperties.Port)
	}

	if servicePlan.RDSProperties.PreferredBackupWindow != "" {
		createDBInstanceInput.PreferredBackupWindow = aws.String(servicePlan.RDSProperties.PreferredBackupWindow)
	}

	if provisionParameters.PreferredBackupWindow != "" {
		createDBInstanceInput.PreferredBackupWindow = aws.String(provisionParameters.PreferredBackupWindow)
	}

	if servicePlan.RDSProperties.PreferredMaintenanceWindow != "" {
		createDBInstanceInput.PreferredMaintenanceWindow = aws.String(servicePlan.RDSProperties.PreferredMaintenanceWindow)
	}

	if provisionParameters.PreferredMaintenanceWindow != "" {
		createDBInstanceInput.PreferredMaintenanceWindow = aws.String(provisionParameters.PreferredMaintenanceWindow)
	}

	createDBInstanceInput.PubliclyAccessible = aws.Bool(servicePlan.RDSProperties.PubliclyAccessible)

	createDBInstanceInput.StorageEncrypted = aws.Bool(servicePlan.RDSProperties.StorageEncrypted)

	if servicePlan.RDSProperties.KmsKeyID != "" {
		createDBInstanceInput.KmsKeyId = aws.String(servicePlan.RDSProperties.KmsKeyID)
	}

	if servicePlan.RDSProperties.StorageType != "" {
		createDBInstanceInput.StorageType = aws.String(servicePlan.RDSProperties.StorageType)
	}

	if servicePlan.RDSProperties.Iops > 0 {
		createDBInstanceInput.Iops = aws.Int64(servicePlan.RDSProperties.Iops)
	}

	createDBInstanceInput.Tags = b.buildInstanceTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID)

	if len(servicePlan.RDSProperties.VpcSecurityGroupIds) > 0 {
		createDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice(servicePlan.RDSProperties.VpcSecurityGroupIds)
	}

	createDBInstanceInput.CopyTagsToSnapshot = aws.Bool(servicePlan.RDSProperties.CopyTagsToSnapshot)

	return createDBInstanceInput
}

func (b *RDSBroker) buildModifyDBInstanceInput(
	instanceID string,
	details brokerapi.UpdateDetails,
	updateParameters UpdateParameters,
	servicePlan ServicePlan,
	dbInstance *rds.DBInstance,
) (*rds.ModifyDBInstanceInput, error) {
	modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
		ApplyImmediately:     aws.Bool(updateParameters.ApplyImmediately),
	}

	if *dbInstance.DBInstanceClass != servicePlan.RDSProperties.DBInstanceClass {
		modifyDBInstanceInput.DBInstanceClass = aws.String(servicePlan.RDSProperties.DBInstanceClass)
	}

	if strings.ToLower(*dbInstance.Engine) != strings.ToLower(servicePlan.RDSProperties.Engine) {
		return modifyDBInstanceInput, fmt.Errorf("This broker does not support updating the RDS engine from '%s' to '%s'", *dbInstance.Engine, servicePlan.RDSProperties.Engine)
	}

	if *dbInstance.EngineVersion != servicePlan.RDSProperties.EngineVersion {
		modifyDBInstanceInput.EngineVersion = aws.String(servicePlan.RDSProperties.EngineVersion)
		modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(b.allowMajorVersionUpgrade(servicePlan.RDSProperties.EngineVersion, *dbInstance.EngineVersion))
	}

	if *dbInstance.AllocatedStorage < servicePlan.RDSProperties.AllocatedStorage {
		modifyDBInstanceInput.AllocatedStorage = aws.Int64(servicePlan.RDSProperties.AllocatedStorage)
	}

	if *dbInstance.AutoMinorVersionUpgrade != servicePlan.RDSProperties.AutoMinorVersionUpgrade {
		modifyDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(servicePlan.RDSProperties.AutoMinorVersionUpgrade)
	}

	if updateParameters.BackupRetentionPeriod > 0 {
		if *dbInstance.BackupRetentionPeriod != updateParameters.BackupRetentionPeriod {
			modifyDBInstanceInput.BackupRetentionPeriod = aws.Int64(updateParameters.BackupRetentionPeriod)
		}
	} else {
		if servicePlan.RDSProperties.BackupRetentionPeriod > 0 {
			if *dbInstance.BackupRetentionPeriod != servicePlan.RDSProperties.BackupRetentionPeriod {
				modifyDBInstanceInput.BackupRetentionPeriod = aws.Int64(servicePlan.RDSProperties.BackupRetentionPeriod)
			}
		}
	}

	if servicePlan.RDSProperties.DBParameterGroupName != "" {
		for _, dbParameterGroup := range dbInstance.DBParameterGroups {
			if *dbParameterGroup.DBParameterGroupName != servicePlan.RDSProperties.DBParameterGroupName {
				modifyDBInstanceInput.DBParameterGroupName = aws.String(servicePlan.RDSProperties.DBParameterGroupName)
				break
			}
		}
	}

	modifyDBSecurityGroups := false
	if len(dbInstance.DBSecurityGroups) != len(servicePlan.RDSProperties.DBSecurityGroups) {
		modifyDBSecurityGroups = true
	} else {
		dbSecurityGroupNames := map[string]struct{}{}

		for _, dbSecurityGroup := range dbInstance.DBSecurityGroups {
			dbSecurityGroupNames[*dbSecurityGroup.DBSecurityGroupName] = struct{}{}
		}

		for _, dbSecurityGroupName := range servicePlan.RDSProperties.DBSecurityGroups {
			if _, ok := dbSecurityGroupNames[dbSecurityGroupName]; !ok {
				modifyDBSecurityGroups = true
				break
			}
		}
	}
	if modifyDBSecurityGroups {
		modifyDBInstanceInput.DBSecurityGroups = aws.StringSlice(servicePlan.RDSProperties.DBSecurityGroups)
	}

	if *dbInstance.MultiAZ != servicePlan.RDSProperties.MultiAZ {
		modifyDBInstanceInput.MultiAZ = aws.Bool(servicePlan.RDSProperties.MultiAZ)
	}

	if servicePlan.RDSProperties.OptionGroupName != "" {
		for _, optionGroupMembership := range dbInstance.OptionGroupMemberships {
			if *optionGroupMembership.OptionGroupName != servicePlan.RDSProperties.OptionGroupName {
				modifyDBInstanceInput.OptionGroupName = aws.String(servicePlan.RDSProperties.OptionGroupName)
				break
			}
		}
	}

	if updateParameters.PreferredBackupWindow != "" {
		if *dbInstance.PreferredBackupWindow != updateParameters.PreferredBackupWindow {
			modifyDBInstanceInput.PreferredBackupWindow = aws.String(updateParameters.PreferredBackupWindow)
		}
	} else {
		if servicePlan.RDSProperties.PreferredBackupWindow != "" {
			if *dbInstance.PreferredBackupWindow != servicePlan.RDSProperties.PreferredBackupWindow {
				modifyDBInstanceInput.PreferredBackupWindow = aws.String(servicePlan.RDSProperties.PreferredBackupWindow)
			}
		}
	}

	if updateParameters.PreferredMaintenanceWindow != "" {
		if *dbInstance.PreferredMaintenanceWindow != updateParameters.PreferredMaintenanceWindow {
			modifyDBInstanceInput.PreferredMaintenanceWindow = aws.String(updateParameters.PreferredMaintenanceWindow)
		}
	} else {
		if servicePlan.RDSProperties.PreferredMaintenanceWindow != "" {
			if *dbInstance.PreferredMaintenanceWindow != servicePlan.RDSProperties.PreferredMaintenanceWindow {
				modifyDBInstanceInput.PreferredMaintenanceWindow = aws.String(servicePlan.RDSProperties.PreferredMaintenanceWindow)
			}
		}
	}

	if servicePlan.RDSProperties.StorageType != "" {
		if *dbInstance.StorageType != servicePlan.RDSProperties.StorageType {
			modifyDBInstanceInput.StorageType = aws.String(servicePlan.RDSProperties.StorageType)
		}
	}

	if servicePlan.RDSProperties.Iops > 0 {
		if *dbInstance.Iops != servicePlan.RDSProperties.Iops {
			modifyDBInstanceInput.Iops = aws.Int64(servicePlan.RDSProperties.Iops)
		}
	}

	modifyVpcSecurityGroupIds := false
	if len(dbInstance.VpcSecurityGroups) != len(servicePlan.RDSProperties.VpcSecurityGroupIds) {
		modifyVpcSecurityGroupIds = true
	} else {
		vpcSecurityGroupIds := map[string]struct{}{}

		for _, vpcSecurityGroup := range dbInstance.VpcSecurityGroups {
			vpcSecurityGroupIds[*vpcSecurityGroup.VpcSecurityGroupId] = struct{}{}
		}

		for _, vpcSecurityGroupID := range servicePlan.RDSProperties.VpcSecurityGroupIds {
			if _, ok := vpcSecurityGroupIds[vpcSecurityGroupID]; !ok {
				modifyVpcSecurityGroupIds = true
				break
			}
		}
	}
	if modifyVpcSecurityGroupIds {
		modifyDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice(servicePlan.RDSProperties.VpcSecurityGroupIds)
	}

	if *dbInstance.CopyTagsToSnapshot != servicePlan.RDSProperties.CopyTagsToSnapshot {
		modifyDBInstanceInput.CopyTagsToSnapshot = aws.Bool(servicePlan.RDSProperties.CopyTagsToSnapshot)
	}

	return modifyDBInstanceInput, nil
}

func (b *RDSBroker) allowMajorVersionUpgrade(newEngineVersion, oldEngineVersion string) bool {
	newSplittedEngineVersion := strings.Split(newEngineVersion, ".")
	newMajorEngineVersion := fmt.Sprintf("%s:%s", newSplittedEngineVersion[0], newSplittedEngineVersion[1])

	oldSplittedEngineVersion := strings.Split(oldEngineVersion, ".")
	oldMajorEngineVersion := fmt.Sprintf("%s:%s", oldSplittedEngineVersion[0], oldSplittedEngineVersion[1])

	if newMajorEngineVersion > oldMajorEngineVersion {
		return true
	}

	return false
}

func (b *RDSBroker) buildDeleteDBInstanceInput(instanceID string, servicePlan ServicePlan) *rds.DeleteDBInstanceInput {
	deleteDBInstanceInput := &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
		SkipFinalSnapshot:    aws.Bool(servicePlan.RDSProperties.SkipFinalSnapshot),
	}

	if !servicePlan.RDSProperties.SkipFinalSnapshot {
		deleteDBInstanceInput.FinalDBSnapshotIdentifier = aws.String(b.dbSnapshotName(instanceID))
	}

	return deleteDBInstanceInput
}

func (b *RDSBroker) connectionString(engine string, dbAddress string, dbPort int64, dbName string, masterUsername string, masterPassword string) (string, error) {
	var connectionString string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		connectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", masterUsername, masterPassword, dbAddress, dbPort, dbName)
	case "postgres":
		connectionString = fmt.Sprintf("host=%s port=%d dbname=%s user='%s' password='%s'", dbAddress, dbPort, dbName, masterUsername, masterPassword)
	default:
		return connectionString, fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	return connectionString, nil
}

func (b *RDSBroker) createDB(db *sql.DB, engine string, dbName string, logger lager.Logger) error {
	dbAlreadyExists, err := b.dbExists(db, engine, dbName, logger)
	if err != nil {
		return err
	}
	if dbAlreadyExists {
		return nil
	}

	var createDBStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		createDBStatement = "CREATE DATABASE IF NOT EXISTS " + dbName
	case "postgres":
		createDBStatement = "CREATE DATABASE \"" + dbName + "\""
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("create-database", lager.Data{"statement": createDBStatement})

	if _, err := db.Exec(createDBStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (b *RDSBroker) dropDB(db *sql.DB, engine string, dbName string, logger lager.Logger) error {
	var dropDBStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		dropDBStatement = "DROP DATABASE IF EXISTS " + dbName
	case "postgres":
		dropDBStatement = "DROP DATABASE IF EXISTS \"" + dbName + "\""
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("drop-database", lager.Data{"statement": dropDBStatement})

	if _, err := db.Exec(dropDBStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (b *RDSBroker) dbExists(db *sql.DB, engine string, dbName string, logger lager.Logger) (bool, error) {
	var selectDatabaseStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		selectDatabaseStatement = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + dbName + "'"
	case "postgres":
		selectDatabaseStatement = "SELECT datname FROM pg_database WHERE datname='" + dbName + "'"
	default:
		return false, fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("db-exists", lager.Data{"statement": selectDatabaseStatement})

	var dummy string
	err := db.QueryRow(selectDatabaseStatement).Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) createUser(db *sql.DB, engine string, dbUsername string, dbPassword string, logger lager.Logger) error {
	var createUserStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		createUserStatement = "CREATE USER '" + dbUsername + "' IDENTIFIED BY '" + dbPassword + "'"
	case "postgres":
		createUserStatement = "CREATE USER \"" + dbUsername + "\" WITH PASSWORD '" + dbPassword + "'"
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("create-user", lager.Data{"statement": createUserStatement})

	if _, err := db.Exec(createUserStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (b *RDSBroker) dropUser(db *sql.DB, engine string, dbUsername string, logger lager.Logger) error {
	var dropUserStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		dropUserStatement = "DROP USER '" + dbUsername + "'@'%'"
	case "postgres":
		dropUserStatement = "DROP USER IF EXISTS \"" + dbUsername + "\""
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	if _, err := db.Exec(dropUserStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (b *RDSBroker) grantPrivileges(db *sql.DB, engine string, dbName string, dbUsername string, logger lager.Logger) error {
	var grantPrivilegesStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		grantPrivilegesStatement = "GRANT ALL PRIVILEGES ON " + dbName + ".* TO '" + dbUsername + "'@'%'"
	case "postgres":
		grantPrivilegesStatement = "GRANT ALL PRIVILEGES ON DATABASE \"" + dbName + "\" TO \"" + dbUsername + "\""
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := db.Exec(grantPrivilegesStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (b *RDSBroker) revokePrivileges(db *sql.DB, engine string, dbName string, dbUsername string, logger lager.Logger) error {
	var revokePrivilegesStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		revokePrivilegesStatement = "REVOKE ALL PRIVILEGES ON " + dbName + ".* from '" + dbUsername + "'@'%'"
	case "postgres":
		revokePrivilegesStatement = "REVOKE ALL PRIVILEGES ON DATABASE \"" + dbName + "\" FROM \"" + dbUsername + "\""
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("revoke-privileges", lager.Data{"statement": revokePrivilegesStatement})

	if _, err := db.Exec(revokePrivilegesStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (b *RDSBroker) dbPrivileges(db *sql.DB, engine string, logger lager.Logger) (map[string][]string, error) {
	privileges := make(map[string][]string)

	var selectPrivilegesStatement string
	switch strings.ToLower(engine) {
	case "mysql", "mariadb":
		selectPrivilegesStatement = "SELECT db, user FROM mysql.db"
	case "postgres":
		selectPrivilegesStatement = "SELECT datname, usename FROM pg_database d, pg_user u WHERE usecreatedb = false AND (SELECT has_database_privilege(u.usename, d.datname, 'create'))"
	default:
		return privileges, fmt.Errorf("This broker does not support RDS engine '%s'", engine)
	}

	logger.Debug("db-privileges", lager.Data{"statement": selectPrivilegesStatement})

	rows, err := db.Query(selectPrivilegesStatement)
	defer rows.Close()

	var dbname, username string
	for rows.Next() {
		err := rows.Scan(&dbname, &username)
		if err != nil {
			return privileges, err
		}
		if _, ok := privileges[dbname]; !ok {
			privileges[dbname] = []string{}
		}
		privileges[dbname] = append(privileges[dbname], username)
	}
	err = rows.Err()
	if err != nil {
		return privileges, err
	}

	logger.Debug("db-privileges", lager.Data{"output": privileges})

	return privileges, nil
}
