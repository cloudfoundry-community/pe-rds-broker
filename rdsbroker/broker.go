package rdsbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/joek/brokerapi"

	"github.com/cloudfoundry-community/pe-rds-broker/awsrds"
	"github.com/cloudfoundry-community/pe-rds-broker/sqlengine"
	"github.com/cloudfoundry-community/pe-rds-broker/utils"
)

const defaultUsernameLength = 16
const defaultPasswordLength = 32

const instanceIDLogKey = "instance-id"
const bindingIDLogKey = "binding-id"
const detailsLogKey = "details"
const acceptsIncompleteLogKey = "acceptsIncomplete"
const asyncAllowedLogKey = "asyncAllowed"
const aurora = "aurora"

var rdsStatus2State = map[string]brokerapi.LastOperationState{
	"available":                    brokerapi.Succeeded,
	"backing-up":                   brokerapi.InProgress,
	"creating":                     brokerapi.InProgress,
	"deleting":                     brokerapi.InProgress,
	"maintenance":                  brokerapi.InProgress,
	"modifying":                    brokerapi.InProgress,
	"rebooting":                    brokerapi.InProgress,
	"renaming":                     brokerapi.InProgress,
	"resetting-master-credentials": brokerapi.InProgress,
	"upgrading":                    brokerapi.InProgress,
}

// RDSBroker implementation
type RDSBroker struct {
	dbPrefix                     string
	allowUserProvisionParameters bool
	allowUserUpdateParameters    bool
	allowUserBindParameters      bool
	masterPasswordSalt           string
	catalog                      Catalog
	dbInstance                   awsrds.DBInstance
	dbCluster                    awsrds.DBCluster
	sqlProvider                  sqlengine.Provider
	logger                       lager.Logger
}

// New create new RDSBroker object
func New(
	config Config,
	dbInstance awsrds.DBInstance,
	dbCluster awsrds.DBCluster,
	sqlProvider sqlengine.Provider,
	logger lager.Logger,
) *RDSBroker {
	return &RDSBroker{
		dbPrefix: config.DBPrefix,

		allowUserProvisionParameters: config.AllowUserProvisionParameters,
		allowUserUpdateParameters:    config.AllowUserUpdateParameters,
		allowUserBindParameters:      config.AllowUserBindParameters,
		masterPasswordSalt:           config.MasterPasswordSalt,
		catalog:                      config.Catalog,
		dbInstance:                   dbInstance,
		dbCluster:                    dbCluster,
		sqlProvider:                  sqlProvider,
		logger:                       logger.Session("broker"),
	}
}

// Services builds brokerapi service catalog
func (b *RDSBroker) Services(context context.Context) []brokerapi.Service {
	services := []brokerapi.Service{}
	for _, s := range b.catalog.Services {
		plans := []brokerapi.ServicePlan{}
		for _, p := range s.Plans {
			plans = append(plans, brokerapi.ServicePlan{
				ID:          p.ID,
				Name:        p.Name,
				Description: p.Description,
				Free:        p.Free,
				Metadata:    p.Metadata,
			})
		}
		services = append(services, brokerapi.Service{
			ID:              s.ID,
			Name:            s.Name,
			Description:     s.Description,
			Bindable:        s.Bindable,
			Tags:            s.Tags,
			PlanUpdatable:   s.PlanUpdateable,
			Plans:           plans,
			Requires:        s.Requires,
			Metadata:        s.Metadata,
			DashboardClient: s.DashboardClient,
		})
	}
	return services
}

// Provision RDSBroker Service
func (b *RDSBroker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	b.logger.Debug("provision", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})
	//TODO: Print propper ops message

	provisioningResponse := brokerapi.ProvisionedServiceSpec{
		IsAsync: true,
	}

	if !asyncAllowed {
		return provisioningResponse, brokerapi.ErrAsyncRequired
	}

	provisionParameters := ProvisionParameters{}
	if b.allowUserProvisionParameters && len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &provisionParameters); err != nil {
			return provisioningResponse, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return provisioningResponse, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	var err error
	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		createDBCluster := b.createDBCluster(instanceID, servicePlan, provisionParameters, details)
		if err = b.dbCluster.Create(b.dbClusterIdentifier(instanceID), *createDBCluster); err != nil {
			return provisioningResponse, err
		}
		defer func() {
			if err != nil {
				b.dbCluster.Delete(b.dbClusterIdentifier(instanceID), servicePlan.RDSProperties.SkipFinalSnapshot)
			}
		}()
	}

	createDBInstance := b.createDBInstance(instanceID, servicePlan, provisionParameters, details)
	if err = b.dbInstance.Create(b.dbInstanceIdentifier(instanceID), *createDBInstance); err != nil {
		return provisioningResponse, err
	}

	return provisioningResponse, nil
}

// Update RDSBroker service
func (b *RDSBroker) Update(context context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	b.logger.Debug("update", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	provisioningResponse := brokerapi.UpdateServiceSpec{
		IsAsync: true,
	}

	if !asyncAllowed {
		return provisioningResponse, brokerapi.ErrAsyncRequired
	}

	updateParameters := UpdateParameters{}
	if b.allowUserUpdateParameters && len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &updateParameters); err != nil {
			return provisioningResponse, err
		}
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return provisioningResponse, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if !service.PlanUpdateable {
		return provisioningResponse, brokerapi.ErrInstanceNotUpdateable
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return provisioningResponse, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		modifyDBCluster := b.modifyDBCluster(instanceID, servicePlan, updateParameters, details)
		if err := b.dbCluster.Modify(b.dbClusterIdentifier(instanceID), *modifyDBCluster, updateParameters.ApplyImmediately); err != nil {
			return provisioningResponse, err
		}
	}

	modifyDBInstance := b.modifyDBInstance(instanceID, servicePlan, updateParameters, details)
	if err := b.dbInstance.Modify(b.dbInstanceIdentifier(instanceID), *modifyDBInstance, updateParameters.ApplyImmediately); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return provisioningResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return provisioningResponse, err
	}

	return provisioningResponse, nil
}

// Deprovision RDSBroker service
func (b *RDSBroker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error) {
	b.logger.Debug("deprovision", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	provisioningResponse := brokerapi.DeprovisionServiceSpec{
		IsAsync: true,
	}

	if !asyncAllowed {
		return provisioningResponse, brokerapi.ErrAsyncRequired
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return provisioningResponse, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	skipDBInstanceFinalSnapshot := servicePlan.RDSProperties.SkipFinalSnapshot
	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		skipDBInstanceFinalSnapshot = true
	}

	if err := b.dbInstance.Delete(b.dbInstanceIdentifier(instanceID), skipDBInstanceFinalSnapshot); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return provisioningResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return provisioningResponse, err
	}

	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		b.dbCluster.Delete(b.dbClusterIdentifier(instanceID), servicePlan.RDSProperties.SkipFinalSnapshot)
	}

	provisioningResponse.OperationData = "Successfull deprovisioned Instance"
	return provisioningResponse, nil
}

// Bind RDSBroker service
func (b *RDSBroker) Bind(context context.Context, instanceID, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	b.logger.Debug("bind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	bindingResponse := brokerapi.Binding{}

	bindParameters := BindParameters{}
	if b.allowUserBindParameters && len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &bindParameters); err != nil {
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

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return bindingResponse, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	var dbAddress, dbName, masterUsername string
	var dbPort int64
	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		dbClusterDetails, err := b.dbCluster.Describe(b.dbClusterIdentifier(instanceID))
		if err != nil {
			if err == awsrds.ErrDBInstanceDoesNotExist {
				return bindingResponse, brokerapi.ErrInstanceDoesNotExist
			}
			return bindingResponse, err
		}

		dbAddress = dbClusterDetails.Endpoint
		dbPort = dbClusterDetails.Port
		masterUsername = dbClusterDetails.MasterUsername
		if dbClusterDetails.DatabaseName != "" {
			dbName = dbClusterDetails.DatabaseName
		} else {
			dbName = b.dbName(instanceID)
		}
	} else {
		dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
		if err != nil {
			if err == awsrds.ErrDBInstanceDoesNotExist {
				return bindingResponse, brokerapi.ErrInstanceDoesNotExist
			}
			return bindingResponse, err
		}

		dbAddress = dbInstanceDetails.Address
		dbPort = dbInstanceDetails.Port
		masterUsername = dbInstanceDetails.MasterUsername
		if dbInstanceDetails.DBName != "" {
			dbName = dbInstanceDetails.DBName
		} else {
			dbName = b.dbName(instanceID)
		}
	}

	sqlEngine, err := b.sqlProvider.GetSQLEngine(servicePlan.RDSProperties.Engine)
	if err != nil {
		return bindingResponse, err
	}

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.masterPassword(instanceID)); err != nil {
		return bindingResponse, err
	}
	defer sqlEngine.Close()

	dbUsername := b.dbUsername(bindingID)
	dbPassword := b.dbPassword()

	if bindParameters.DBName != "" {
		dbName = bindParameters.DBName
		if err = sqlEngine.CreateDB(dbName); err != nil {
			return bindingResponse, err
		}
	}

	if err = sqlEngine.CreateUser(dbUsername, dbPassword); err != nil {
		return bindingResponse, err
	}

	if err = sqlEngine.GrantPrivileges(dbName, dbUsername); err != nil {
		return bindingResponse, err
	}

	bindingResponse.Credentials = &CredentialsHash{
		Host:     dbAddress,
		Port:     dbPort,
		Name:     dbName,
		Username: dbUsername,
		Password: dbPassword,
		URI:      sqlEngine.URI(dbAddress, dbPort, dbName, dbUsername, dbPassword),
		JDBCURI:  sqlEngine.JDBCURI(dbAddress, dbPort, dbName, dbUsername, dbPassword),
	}

	return bindingResponse, nil
}

// Unbind RDSBroker service
func (b *RDSBroker) Unbind(context context.Context, instanceID, bindingID string, details brokerapi.UnbindDetails) error {
	b.logger.Debug("unbind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	var dbAddress, dbName, masterUsername string
	var dbPort int64
	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		dbClusterDetails, err := b.dbCluster.Describe(b.dbClusterIdentifier(instanceID))
		if err != nil {
			if err == awsrds.ErrDBInstanceDoesNotExist {
				return brokerapi.ErrInstanceDoesNotExist
			}
			return err
		}

		dbAddress = dbClusterDetails.Endpoint
		dbPort = dbClusterDetails.Port
		masterUsername = dbClusterDetails.MasterUsername
		if dbClusterDetails.DatabaseName != "" {
			dbName = dbClusterDetails.DatabaseName
		} else {
			dbName = b.dbName(instanceID)
		}
	} else {
		dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
		if err != nil {
			if err == awsrds.ErrDBInstanceDoesNotExist {
				return brokerapi.ErrInstanceDoesNotExist
			}
			return err
		}

		dbAddress = dbInstanceDetails.Address
		dbPort = dbInstanceDetails.Port
		masterUsername = dbInstanceDetails.MasterUsername
		if dbInstanceDetails.DBName != "" {
			dbName = dbInstanceDetails.DBName
		} else {
			dbName = b.dbName(instanceID)
		}
	}

	sqlEngine, err := b.sqlProvider.GetSQLEngine(servicePlan.RDSProperties.Engine)
	if err != nil {
		return err
	}

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.masterPassword(instanceID)); err != nil {
		return err
	}
	defer sqlEngine.Close()

	privileges, err := sqlEngine.Privileges()
	if err != nil {
		return err
	}

	var userDB string
	dbUsername := b.dbUsername(bindingID)
	for privDBName, userNames := range privileges {
		for _, userName := range userNames {
			if userName == dbUsername {
				userDB = privDBName
				break
			}
		}
	}

	if userDB != "" {
		if err = sqlEngine.RevokePrivileges(userDB, dbUsername); err != nil {
			return err
		}

		if userDB != dbName {
			users := privileges[userDB]
			if len(users) == 1 {
				if err = sqlEngine.DropDB(userDB); err != nil {
					return err
				}
			}
		}
	}

	if err = sqlEngine.DropUser(dbUsername); err != nil {
		return err
	}

	return nil
}

// LastOperation for DB on AWS
func (b *RDSBroker) LastOperation(context context.Context, instanceID string, operationData string) (brokerapi.LastOperation, error) {
	b.logger.Debug("last-operation", lager.Data{
		instanceIDLogKey: instanceID,
	})

	lastOperationResponse := brokerapi.LastOperation{State: brokerapi.Failed}

	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return lastOperationResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return lastOperationResponse, err
	}

	lastOperationResponse.Description = fmt.Sprintf("DB Instance '%s' status is '%s'", b.dbInstanceIdentifier(instanceID), dbInstanceDetails.Status)

	if state, ok := rdsStatus2State[dbInstanceDetails.Status]; ok {
		lastOperationResponse.State = state
	}

	if lastOperationResponse.State == brokerapi.Succeeded && dbInstanceDetails.PendingModifications {
		lastOperationResponse.State = brokerapi.InProgress
		lastOperationResponse.Description = fmt.Sprintf("DB Instance '%s' has pending modifications", b.dbInstanceIdentifier(instanceID))
	}

	return lastOperationResponse, nil
}

func (b *RDSBroker) dbClusterIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", b.dbPrefix, strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSBroker) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", b.dbPrefix, strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSBroker) masterUsername() string {
	return utils.RandomAlphaNum(defaultUsernameLength)
}

func (b *RDSBroker) masterPassword(instanceID string) string {
	if b.masterPasswordSalt != "" {
		return utils.GetMD5B64(instanceID, defaultPasswordLength, b.masterPasswordSalt)
	}
	return utils.GetMD5B64(instanceID, defaultPasswordLength)
}

func (b *RDSBroker) dbUsername(bindingID string) string {
	return utils.GetMD5B64(bindingID, defaultUsernameLength)
}

func (b *RDSBroker) dbPassword() string {
	return utils.RandomAlphaNum(defaultPasswordLength)
}

func (b *RDSBroker) dbName(instanceID string) string {
	return fmt.Sprintf("%s_%s", b.dbPrefix, strings.Replace(instanceID, "-", "_", -1))
}

func (b *RDSBroker) createDBCluster(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) *awsrds.DBClusterDetails {
	dbClusterDetails := b.dbClusterFromPlan(servicePlan)
	dbClusterDetails.DatabaseName = b.dbName(instanceID)
	dbClusterDetails.MasterUsername = b.masterUsername()
	dbClusterDetails.MasterUserPassword = b.masterPassword(instanceID)

	if provisionParameters.BackupRetentionPeriod > 0 {
		dbClusterDetails.BackupRetentionPeriod = provisionParameters.BackupRetentionPeriod
	}

	if provisionParameters.DBName != "" {
		dbClusterDetails.DatabaseName = provisionParameters.DBName
	}

	if provisionParameters.PreferredBackupWindow != "" {
		dbClusterDetails.PreferredBackupWindow = provisionParameters.PreferredBackupWindow
	}

	if provisionParameters.PreferredMaintenanceWindow != "" {
		dbClusterDetails.PreferredMaintenanceWindow = provisionParameters.PreferredMaintenanceWindow
	}

	dbClusterDetails.Tags = b.dbTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID)

	return dbClusterDetails
}

func (b *RDSBroker) modifyDBCluster(instanceID string, servicePlan ServicePlan, updateParameters UpdateParameters, details brokerapi.UpdateDetails) *awsrds.DBClusterDetails {
	dbClusterDetails := b.dbClusterFromPlan(servicePlan)

	if updateParameters.BackupRetentionPeriod > 0 {
		dbClusterDetails.BackupRetentionPeriod = updateParameters.BackupRetentionPeriod
	}

	if updateParameters.PreferredBackupWindow != "" {
		dbClusterDetails.PreferredBackupWindow = updateParameters.PreferredBackupWindow
	}

	if updateParameters.PreferredMaintenanceWindow != "" {
		dbClusterDetails.PreferredMaintenanceWindow = updateParameters.PreferredMaintenanceWindow
	}

	dbClusterDetails.Tags = b.dbTags("Updated", details.ServiceID, details.PlanID, "", "")

	return dbClusterDetails
}

func (b *RDSBroker) dbClusterFromPlan(servicePlan ServicePlan) *awsrds.DBClusterDetails {
	dbClusterDetails := &awsrds.DBClusterDetails{
		Engine: servicePlan.RDSProperties.Engine,
	}

	if servicePlan.RDSProperties.AvailabilityZone != "" {
		dbClusterDetails.AvailabilityZones = []string{servicePlan.RDSProperties.AvailabilityZone}
	}

	if servicePlan.RDSProperties.BackupRetentionPeriod > 0 {
		dbClusterDetails.BackupRetentionPeriod = servicePlan.RDSProperties.BackupRetentionPeriod
	}

	if servicePlan.RDSProperties.DBClusterParameterGroupName != "" {
		dbClusterDetails.DBClusterParameterGroupName = servicePlan.RDSProperties.DBClusterParameterGroupName
	}

	if servicePlan.RDSProperties.DBSubnetGroupName != "" {
		dbClusterDetails.DBSubnetGroupName = servicePlan.RDSProperties.DBSubnetGroupName
	}

	if servicePlan.RDSProperties.EngineVersion != "" {
		dbClusterDetails.EngineVersion = servicePlan.RDSProperties.EngineVersion
	}

	if servicePlan.RDSProperties.Port > 0 {
		dbClusterDetails.Port = servicePlan.RDSProperties.Port
	}

	if servicePlan.RDSProperties.PreferredBackupWindow != "" {
		dbClusterDetails.PreferredBackupWindow = servicePlan.RDSProperties.PreferredBackupWindow
	}

	if servicePlan.RDSProperties.PreferredMaintenanceWindow != "" {
		dbClusterDetails.PreferredMaintenanceWindow = servicePlan.RDSProperties.PreferredMaintenanceWindow
	}

	if len(servicePlan.RDSProperties.VpcSecurityGroupIds) > 0 {
		dbClusterDetails.VpcSecurityGroupIds = servicePlan.RDSProperties.VpcSecurityGroupIds
	}

	return dbClusterDetails
}

func (b *RDSBroker) createDBInstance(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) *awsrds.DBInstanceDetails {
	dbInstanceDetails := b.dbInstanceFromPlan(servicePlan)

	if strings.ToLower(servicePlan.RDSProperties.Engine) == aurora {
		dbInstanceDetails.DBClusterIdentifier = b.dbClusterIdentifier(instanceID)
	} else {
		dbInstanceDetails.DBName = b.dbName(instanceID)
		dbInstanceDetails.MasterUsername = b.masterUsername()
		dbInstanceDetails.MasterUserPassword = b.masterPassword(instanceID)

		if provisionParameters.BackupRetentionPeriod > 0 {
			dbInstanceDetails.BackupRetentionPeriod = provisionParameters.BackupRetentionPeriod
		}

		if provisionParameters.CharacterSetName != "" {
			dbInstanceDetails.CharacterSetName = provisionParameters.CharacterSetName
		}

		if provisionParameters.DBName != "" {
			dbInstanceDetails.DBName = provisionParameters.DBName
		}

		if provisionParameters.PreferredBackupWindow != "" {
			dbInstanceDetails.PreferredBackupWindow = provisionParameters.PreferredBackupWindow
		}
	}

	if provisionParameters.PreferredMaintenanceWindow != "" {
		dbInstanceDetails.PreferredMaintenanceWindow = provisionParameters.PreferredMaintenanceWindow
	}

	dbInstanceDetails.Tags = b.dbTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID)

	return dbInstanceDetails
}

func (b *RDSBroker) modifyDBInstance(instanceID string, servicePlan ServicePlan, updateParameters UpdateParameters, details brokerapi.UpdateDetails) *awsrds.DBInstanceDetails {
	dbInstanceDetails := b.dbInstanceFromPlan(servicePlan)

	if strings.ToLower(servicePlan.RDSProperties.Engine) != aurora {
		if updateParameters.BackupRetentionPeriod > 0 {
			dbInstanceDetails.BackupRetentionPeriod = updateParameters.BackupRetentionPeriod
		}

		if updateParameters.PreferredBackupWindow != "" {
			dbInstanceDetails.PreferredBackupWindow = updateParameters.PreferredBackupWindow
		}
	}

	if updateParameters.PreferredMaintenanceWindow != "" {
		dbInstanceDetails.PreferredMaintenanceWindow = updateParameters.PreferredMaintenanceWindow
	}

	dbInstanceDetails.Tags = b.dbTags("Updated", details.ServiceID, details.PlanID, "", "")

	return dbInstanceDetails
}

func (b *RDSBroker) dbInstanceFromPlan(servicePlan ServicePlan) *awsrds.DBInstanceDetails {
	dbInstanceDetails := &awsrds.DBInstanceDetails{
		DBInstanceClass: servicePlan.RDSProperties.DBInstanceClass,
		Engine:          servicePlan.RDSProperties.Engine,
	}

	dbInstanceDetails.AutoMinorVersionUpgrade = servicePlan.RDSProperties.AutoMinorVersionUpgrade

	if servicePlan.RDSProperties.AvailabilityZone != "" {
		dbInstanceDetails.AvailabilityZone = servicePlan.RDSProperties.AvailabilityZone
	}

	dbInstanceDetails.CopyTagsToSnapshot = servicePlan.RDSProperties.CopyTagsToSnapshot

	if servicePlan.RDSProperties.DBParameterGroupName != "" {
		dbInstanceDetails.DBParameterGroupName = servicePlan.RDSProperties.DBParameterGroupName
	}

	if servicePlan.RDSProperties.DBSubnetGroupName != "" {
		dbInstanceDetails.DBSubnetGroupName = servicePlan.RDSProperties.DBSubnetGroupName
	}

	if servicePlan.RDSProperties.EngineVersion != "" {
		dbInstanceDetails.EngineVersion = servicePlan.RDSProperties.EngineVersion
	}

	if servicePlan.RDSProperties.OptionGroupName != "" {
		dbInstanceDetails.OptionGroupName = servicePlan.RDSProperties.OptionGroupName
	}

	if servicePlan.RDSProperties.PreferredMaintenanceWindow != "" {
		dbInstanceDetails.PreferredMaintenanceWindow = servicePlan.RDSProperties.PreferredMaintenanceWindow
	}

	dbInstanceDetails.PubliclyAccessible = servicePlan.RDSProperties.PubliclyAccessible

	if strings.ToLower(servicePlan.RDSProperties.Engine) != aurora {
		if servicePlan.RDSProperties.AllocatedStorage > 0 {
			dbInstanceDetails.AllocatedStorage = servicePlan.RDSProperties.AllocatedStorage
		}

		if servicePlan.RDSProperties.BackupRetentionPeriod > 0 {
			dbInstanceDetails.BackupRetentionPeriod = servicePlan.RDSProperties.BackupRetentionPeriod
		}

		if servicePlan.RDSProperties.CharacterSetName != "" {
			dbInstanceDetails.CharacterSetName = servicePlan.RDSProperties.CharacterSetName
		}

		if len(servicePlan.RDSProperties.DBSecurityGroups) > 0 {
			dbInstanceDetails.DBSecurityGroups = servicePlan.RDSProperties.DBSecurityGroups
		}

		if servicePlan.RDSProperties.Iops > 0 {
			dbInstanceDetails.Iops = servicePlan.RDSProperties.Iops
		}

		if servicePlan.RDSProperties.KmsKeyID != "" {
			dbInstanceDetails.KmsKeyID = servicePlan.RDSProperties.KmsKeyID
		}

		if servicePlan.RDSProperties.LicenseModel != "" {
			dbInstanceDetails.LicenseModel = servicePlan.RDSProperties.LicenseModel
		}

		dbInstanceDetails.MultiAZ = servicePlan.RDSProperties.MultiAZ

		if servicePlan.RDSProperties.Port > 0 {
			dbInstanceDetails.Port = servicePlan.RDSProperties.Port
		}

		if servicePlan.RDSProperties.PreferredBackupWindow != "" {
			dbInstanceDetails.PreferredBackupWindow = servicePlan.RDSProperties.PreferredBackupWindow
		}

		dbInstanceDetails.StorageEncrypted = servicePlan.RDSProperties.StorageEncrypted

		if servicePlan.RDSProperties.StorageType != "" {
			dbInstanceDetails.StorageType = servicePlan.RDSProperties.StorageType
		}

		if len(servicePlan.RDSProperties.VpcSecurityGroupIds) > 0 {
			dbInstanceDetails.VpcSecurityGroupIds = servicePlan.RDSProperties.VpcSecurityGroupIds
		}
	}

	return dbInstanceDetails
}

func (b *RDSBroker) dbTags(action, serviceID, planID, organizationID, spaceID string) map[string]string {
	tags := make(map[string]string)

	tags["Owner"] = "Cloud Foundry"

	tags[action+" by"] = "AWS RDS Service Broker"

	tags[action+" at"] = time.Now().Format(time.RFC822Z)

	if serviceID != "" {
		tags["Service ID"] = serviceID
	}

	if planID != "" {
		tags["Plan ID"] = planID
	}

	if organizationID != "" {
		tags["Organization ID"] = organizationID
	}

	if spaceID != "" {
		tags["Space ID"] = spaceID
	}

	return tags
}
