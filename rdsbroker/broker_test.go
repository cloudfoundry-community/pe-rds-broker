package rdsbroker_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-community/pe-rds-broker/rdsbroker"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/joek/brokerapi"

	"github.com/cloudfoundry-community/pe-rds-broker/awsrds"
	rdsfake "github.com/cloudfoundry-community/pe-rds-broker/awsrds/fakes"
	sqlfake "github.com/cloudfoundry-community/pe-rds-broker/sqlengine/fakes"
)

var _ = Describe("RDS Broker", func() {
	var (
		rdsProperties1 RDSProperties
		rdsProperties2 RDSProperties
		plan1          ServicePlan
		plan2          ServicePlan
		service1       Service
		service2       Service
		catalog        Catalog

		config Config

		dbInstance *rdsfake.FakeDBInstance
		dbCluster  *rdsfake.FakeDBCluster

		sqlProvider *sqlfake.FakeProvider
		sqlEngine   *sqlfake.FakeSQLEngine

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsBroker *RDSBroker

		context = context.Background()

		allowUserProvisionParameters bool
		allowUserUpdateParameters    bool
		allowUserBindParameters      bool
		serviceBindable              bool
		planUpdateable               bool
		skipFinalSnapshot            bool
		serviceBrokerID              string

		instanceID                     = "instance-id"
		bindingID                      = "binding-id"
		dbInstanceIdentifier           = "cf-instance-id"
		dbClusterIdentifier            = "cf-instance-id"
		dbName                         = "cf_instance_id"
		dbUsername                     = "YmluZGluZy1pZNQd"
		masterUserPassword             = "aW5zdGFuY2UtaWTUHYzZjwCyBOm"
		masterUserPasswordWithSalt     = "aWFuV3M1dHphZG5HY0ZldS1ZaTJkVdQd"
		masterUserPasswordSHA2         = "xWAlaiUlLgZZv0v5vXqd7GsUENhU06tj"
		masterUserPasswordWithSaltSHA2 = "CT45/lEILP83HUUflyn6wDWnWqNU3zAl"
		masterUserSalt                 = "aW5zdGFuY2UtaWTUHYzZjwCyBOm"
	)

	BeforeEach(func() {
		allowUserProvisionParameters = true
		allowUserUpdateParameters = true
		allowUserBindParameters = true
		serviceBindable = true
		planUpdateable = true
		skipFinalSnapshot = true
		serviceBrokerID = ""

		dbInstance = &rdsfake.FakeDBInstance{}
		dbCluster = &rdsfake.FakeDBCluster{}

		sqlProvider = &sqlfake.FakeProvider{}
		sqlEngine = &sqlfake.FakeSQLEngine{}
		sqlProvider.GetSQLEngineSQLEngine = sqlEngine

		rdsProperties1 = RDSProperties{
			DBInstanceClass:   "db.m1.test",
			Engine:            "test-engine-1",
			EngineVersion:     "1.2.3",
			AllocatedStorage:  100,
			SkipFinalSnapshot: skipFinalSnapshot,
		}

		rdsProperties2 = RDSProperties{
			DBInstanceClass:   "db.m2.test",
			Engine:            "test-engine-2",
			EngineVersion:     "4.5.6",
			AllocatedStorage:  200,
			SkipFinalSnapshot: skipFinalSnapshot,
		}
	})

	JustBeforeEach(func() {
		f := false
		plan1 = ServicePlan{
			ID:            "Plan-1",
			Name:          "Plan 1",
			Description:   "This is the Plan 1",
			Free:          &f,
			RDSProperties: rdsProperties1,
		}
		plan2 = ServicePlan{
			ID:            "Plan-2",
			Name:          "Plan 2",
			Description:   "This is the Plan 2",
			Free:          &f,
			RDSProperties: rdsProperties2,
		}

		service1 = Service{
			ID:             "Service-1",
			Name:           "Service 1",
			Description:    "This is the Service 1",
			Bindable:       serviceBindable,
			PlanUpdateable: planUpdateable,
			Plans:          []ServicePlan{plan1},
		}
		service2 = Service{
			ID:             "Service-2",
			Name:           "Service 2",
			Description:    "This is the Service 2",
			Bindable:       serviceBindable,
			PlanUpdateable: planUpdateable,
			Plans:          []ServicePlan{plan2},
		}

		catalog = Catalog{
			Services: []Service{service1, service2},
		}

		config = Config{
			Region:                       "rds-region",
			DBPrefix:                     "cf",
			AllowUserProvisionParameters: allowUserProvisionParameters,
			AllowUserUpdateParameters:    allowUserUpdateParameters,
			AllowUserBindParameters:      allowUserBindParameters,
			ServiceBrokerID:              serviceBrokerID,
			Catalog:                      catalog,
		}

		logger = lager.NewLogger("rdsbroker_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsBroker = New(config, dbInstance, dbCluster, sqlProvider, logger)
	})

	var _ = Describe("Services", func() {
		var (
			properServicesResponse []brokerapi.Service
		)

		BeforeEach(func() {
			f := false
			properServicesResponse = []brokerapi.Service{
				brokerapi.Service{
					ID:            "Service-1",
					Name:          "Service 1",
					Description:   "This is the Service 1",
					Bindable:      serviceBindable,
					PlanUpdatable: planUpdateable,
					Plans: []brokerapi.ServicePlan{
						brokerapi.ServicePlan{
							ID:          "Plan-1",
							Name:        "Plan 1",
							Description: "This is the Plan 1",
							Free:        &f,
						},
					},
				},
				brokerapi.Service{
					ID:            "Service-2",
					Name:          "Service 2",
					Description:   "This is the Service 2",
					Bindable:      serviceBindable,
					PlanUpdatable: planUpdateable,
					Plans: []brokerapi.ServicePlan{
						brokerapi.ServicePlan{
							ID:          "Plan-2",
							Name:        "Plan 2",
							Description: "This is the Plan 2",
							Free:        &f,
						},
					},
				},
			}
		})

		It("returns the proper CatalogResponse", func() {
			brokerCatalog := rdsBroker.Services(context)
			Expect(brokerCatalog).To(Equal(properServicesResponse))
		})

	})

	var _ = Describe("Provision", func() {
		var (
			provisionDetails brokerapi.ProvisionDetails
			asyncAllowed     bool

			properProvisioningResponse brokerapi.ProvisionedServiceSpec
		)

		BeforeEach(func() {
			provisionDetails = brokerapi.ProvisionDetails{
				OrganizationGUID: "organization-id",
				PlanID:           "Plan-1",
				ServiceID:        "Service-1",
				SpaceGUID:        "space-id",
				RawParameters:    []byte(`{}`),
			}
			asyncAllowed = true

			properProvisioningResponse = brokerapi.ProvisionedServiceSpec{
				IsAsync: true,
			}
		})

		It("Deals with empty Parameters", func() {
			provisionDetails.RawParameters = []byte{}
			_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns the proper response", func() {
			provisioningResponse, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
			Expect(provisioningResponse).To(Equal(properProvisioningResponse))
			Expect(provisioningResponse.IsAsync).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
			Expect(dbInstance.CreateCalled).To(BeTrue())
			Expect(dbInstance.CreateID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.CreateDBInstanceDetails.DBInstanceClass).To(Equal("db.m1.test"))
			Expect(dbInstance.CreateDBInstanceDetails.Engine).To(Equal("test-engine-1"))
			Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal(dbName))
			Expect(dbInstance.CreateDBInstanceDetails.MasterUsername).ToNot(BeEmpty())
			Expect(dbInstance.CreateDBInstanceDetails.MasterUserPassword).To(Equal(masterUserPassword))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Created by"]).To(Equal("AWS RDS Service Broker"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags).To(HaveKey("Created at"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-1"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-1"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Organization ID"]).To(Equal("organization-id"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Space ID"]).To(Equal("space-id"))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("wehn has ServiceBrokerID set", func() {
			BeforeEach(func() {
				serviceBrokerID = "AwsomeID"
			})

			It("Is setting right Owner value", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Owner"]).To(Equal("AwsomeID"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when a password salt is set", func() {

			var rdsBroker *RDSBroker

			BeforeEach(func() {
				config.MasterPasswordSalt = masterUserSalt
				rdsBroker = New(config, dbInstance, dbCluster, sqlProvider, logger)
			})

			It("uses right passwords", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.MasterUserPassword).To(Equal(masterUserPasswordWithSalt))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when master_password_sha2 is set to true", func() {

			var rdsBroker *RDSBroker

			BeforeEach(func() {
				config.MasterPasswordSHA2 = true
				rdsBroker = New(config, dbInstance, dbCluster, sqlProvider, logger)
			})

			It("uses right passwords", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.MasterUserPassword).To(Equal(masterUserPasswordSHA2))
				Expect(err).ToNot(HaveOccurred())
			})
			Context("when a password salt is set", func() {
				BeforeEach(func() {
					config.MasterPasswordSalt = masterUserSalt
					rdsBroker = New(config, dbInstance, dbCluster, sqlProvider, logger)
				})

				It("uses right passwords", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.MasterUserPassword).To(Equal(masterUserPasswordWithSaltSHA2))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				rdsProperties1.AllocatedStorage = int64(100)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.AllocatedStorage).To(Equal(int64(100)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.AllocatedStorage).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				rdsProperties1.AutoMinorVersionUpgrade = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.AutoMinorVersionUpgrade).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				rdsProperties1.AvailabilityZone = "test-az"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
					Expect(dbCluster.CreateDBClusterDetails.AvailabilityZones).To(Equal([]string{"test-az"}))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				rdsProperties1.BackupRetentionPeriod = int64(7)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(7)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.BackupRetentionPeriod).To(Equal(int64(7)))
					Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has BackupRetentionPeriod Parameter", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = []byte(`{"backup_retention_period": 12}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(12)))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties1.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
						Expect(dbCluster.CreateDBClusterDetails.BackupRetentionPeriod).To(Equal(int64(12)))
						Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(0)))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				rdsProperties1.CharacterSetName = "test-characterset-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has CharacterSetName Parameter", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = []byte(`{"character_set_name": "test-characterset-name-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties1.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
						Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal(""))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				rdsProperties1.CopyTagsToSnapshot = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.CopyTagsToSnapshot).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBName parameter", func() {
			BeforeEach(func() {
				provisionDetails.RawParameters = []byte(`{"dbname": "test-dbname"}`)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal("test-dbname"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.DatabaseName).To(Equal("test-dbname"))
					Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				rdsProperties1.DBParameterGroupName = "test-db-parameter-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.DBParameterGroupName).To(Equal("test-db-parameter-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				rdsProperties1.DBSecurityGroups = []string{"test-db-security-group"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.DBSecurityGroups).To(BeNil())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				rdsProperties1.DBSubnetGroupName = "test-db-subnet-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				rdsProperties1.EngineVersion = "1.2.3"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
					Expect(dbCluster.CreateDBClusterDetails.EngineVersion).To(Equal("1.2.3"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				rdsProperties1.Iops = int64(1000)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.Iops).To(Equal(int64(1000)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.Iops).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has KmsKeyID", func() {
			BeforeEach(func() {
				rdsProperties1.KmsKeyID = "test-kms-key-id"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.KmsKeyID).To(Equal("test-kms-key-id"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.KmsKeyID).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				rdsProperties1.LicenseModel = "test-license-model"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.LicenseModel).To(Equal("test-license-model"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.LicenseModel).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				rdsProperties1.MultiAZ = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.MultiAZ).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.MultiAZ).To(BeFalse())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				rdsProperties1.OptionGroupName = "test-option-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.OptionGroupName).To(Equal("test-option-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				rdsProperties1.Port = int64(3306)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.Port).To(Equal(int64(3306)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.Port).To(Equal(int64(3306)))
					Expect(dbInstance.CreateDBInstanceDetails.Port).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				rdsProperties1.PreferredBackupWindow = "test-preferred-backup-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
					Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has PreferredBackupWindow Parameter", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = []byte(`{"preferred_backup_window": "test-preferred-backup-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties1.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
						Expect(dbCluster.CreateDBClusterDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
						Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal(""))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				rdsProperties1.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has PreferredMaintenanceWindow Parameter", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = []byte(`{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties1.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
						Expect(dbCluster.CreateDBClusterDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				rdsProperties1.PubliclyAccessible = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.PubliclyAccessible).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageEncrypted", func() {
			BeforeEach(func() {
				rdsProperties1.StorageEncrypted = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.StorageEncrypted).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.StorageEncrypted).To(BeFalse())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				rdsProperties1.StorageType = "test-storage-type"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.StorageType).To(Equal("test-storage-type"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbInstance.CreateDBInstanceDetails.StorageType).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				rdsProperties1.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbInstance.CreateDBInstanceDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
					Expect(dbInstance.CreateDBInstanceDetails.VpcSecurityGroupIds).To(BeNil())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				asyncAllowed = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				provisionDetails.RawParameters = []byte(`{"backup_retention_period": "invalid"}`)
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("json: cannot unmarshal string into Go value of type int64"))
			})

			Context("and user provision parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserProvisionParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				provisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when creating the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.CreateError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when Engine is Aurora", func() {
			BeforeEach(func() {
				rdsProperties1.Engine = "aurora"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
				Expect(dbCluster.CreateCalled).To(BeTrue())
				Expect(dbCluster.CreateID).To(Equal(dbClusterIdentifier))
				Expect(dbCluster.CreateDBClusterDetails.Engine).To(Equal("aurora"))
				Expect(dbCluster.CreateDBClusterDetails.DatabaseName).To(Equal(dbName))
				Expect(dbCluster.CreateDBClusterDetails.MasterUsername).ToNot(BeEmpty())
				Expect(dbCluster.CreateDBClusterDetails.MasterUserPassword).To(Equal(masterUserPassword))
				Expect(dbCluster.CreateDBClusterDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
				Expect(dbCluster.CreateDBClusterDetails.Tags["Created by"]).To(Equal("AWS RDS Service Broker"))
				Expect(dbCluster.CreateDBClusterDetails.Tags).To(HaveKey("Created at"))
				Expect(dbCluster.CreateDBClusterDetails.Tags["Service ID"]).To(Equal("Service-1"))
				Expect(dbCluster.CreateDBClusterDetails.Tags["Plan ID"]).To(Equal("Plan-1"))
				Expect(dbCluster.CreateDBClusterDetails.Tags["Organization ID"]).To(Equal("organization-id"))
				Expect(dbCluster.CreateDBClusterDetails.Tags["Space ID"]).To(Equal("space-id"))
				Expect(dbInstance.CreateDBInstanceDetails.DBClusterIdentifier).To(Equal(dbClusterIdentifier))
				Expect(dbCluster.DeleteCalled).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when has ServiceBrokerID set", func() {
				BeforeEach(func() {
					serviceBrokerID = "AwsomeID"
				})

				It("Is setting right Owner value", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.Tags["Owner"]).To(Equal("AwsomeID"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has DBClusterParameterGroupName", func() {
				BeforeEach(func() {
					rdsProperties1.DBClusterParameterGroupName = "test-db-cluster-parameter-group-name"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(dbCluster.CreateDBClusterDetails.DBClusterParameterGroupName).To(Equal("test-db-cluster-parameter-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when creating the DB Instance fails", func() {
				BeforeEach(func() {
					dbInstance.CreateError = errors.New("operation failed")
				})

				It("deletes the DB Cluster", func() {
					_, err := rdsBroker.Provision(context, instanceID, provisionDetails, asyncAllowed)
					Expect(err).To(HaveOccurred())
					Expect(dbCluster.DeleteCalled).To(BeTrue())
					Expect(dbCluster.DeleteID).To(Equal(dbClusterIdentifier))
				})
			})
		})
	})

	var _ = Describe("Update", func() {
		var (
			updateDetails brokerapi.UpdateDetails
			asyncAllowed  bool
		)

		BeforeEach(func() {
			updateDetails = brokerapi.UpdateDetails{
				ServiceID:     "Service-2",
				PlanID:        "Plan-2",
				RawParameters: []byte(`{}`),
				PreviousValues: brokerapi.PreviousValues{
					PlanID:    "Plan-1",
					ServiceID: "Service-1",
					OrgID:     "organization-id",
					SpaceID:   "space-id",
				},
			}
			asyncAllowed = true
		})

		It("Deals with empty Parameters", func() {
			updateDetails.RawParameters = []byte{}
			_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns the proper response", func() {
			resp, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
			Expect(resp.IsAsync).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
			Expect(dbInstance.ModifyCalled).To(BeTrue())
			Expect(dbInstance.ModifyID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.ModifyDBInstanceDetails.DBInstanceClass).To(Equal("db.m2.test"))
			Expect(dbInstance.ModifyDBInstanceDetails.Engine).To(Equal("test-engine-2"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Updated by"]).To(Equal("AWS RDS Service Broker"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags).To(HaveKey("Updated at"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-2"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-2"))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("wehn has ServiceBrokerID set", func() {
			BeforeEach(func() {
				serviceBrokerID = "AwsomeID"
			})

			It("Is setting right Owner value", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.Tags["Owner"]).To(Equal("AwsomeID"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				rdsProperties2.AllocatedStorage = int64(100)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.AllocatedStorage).To(Equal(int64(100)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.AllocatedStorage).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				rdsProperties2.AutoMinorVersionUpgrade = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.AutoMinorVersionUpgrade).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				rdsProperties2.AvailabilityZone = "test-az"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
					Expect(dbCluster.ModifyDBClusterDetails.AvailabilityZones).To(Equal([]string{"test-az"}))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				rdsProperties2.BackupRetentionPeriod = int64(7)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(7)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.BackupRetentionPeriod).To(Equal(int64(7)))
					Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has BackupRetentionPeriod Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = []byte(`{"backup_retention_period": 12}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(12)))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties2.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
						Expect(dbCluster.ModifyDBClusterDetails.BackupRetentionPeriod).To(Equal(int64(12)))
						Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(0)))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				rdsProperties2.CharacterSetName = "test-characterset-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.CharacterSetName).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				rdsProperties2.CopyTagsToSnapshot = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.CopyTagsToSnapshot).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBParameterGroupName = "test-db-parameter-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.DBParameterGroupName).To(Equal("test-db-parameter-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				rdsProperties2.DBSecurityGroups = []string{"test-db-security-group"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.DBSecurityGroups).To(BeNil())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBSubnetGroupName = "test-db-subnet-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				rdsProperties2.EngineVersion = "1.2.3"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
					Expect(dbCluster.ModifyDBClusterDetails.EngineVersion).To(Equal("1.2.3"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				rdsProperties2.Iops = int64(1000)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.Iops).To(Equal(int64(1000)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.Iops).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has KmsKeyID", func() {
			BeforeEach(func() {
				rdsProperties2.KmsKeyID = "test-kms-key-id"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.KmsKeyID).To(Equal("test-kms-key-id"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.KmsKeyID).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				rdsProperties2.LicenseModel = "test-license-model"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.LicenseModel).To(Equal("test-license-model"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.LicenseModel).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				rdsProperties2.MultiAZ = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.MultiAZ).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.MultiAZ).To(BeFalse())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.OptionGroupName = "test-option-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.OptionGroupName).To(Equal("test-option-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				rdsProperties2.Port = int64(3306)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.Port).To(Equal(int64(3306)))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.Port).To(Equal(int64(3306)))
					Expect(dbInstance.ModifyDBInstanceDetails.Port).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredBackupWindow = "test-preferred-backup-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
					Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has PreferredBackupWindow Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = []byte(`{"preferred_backup_window": "test-preferred-backup-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties2.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
						Expect(dbCluster.ModifyDBClusterDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
						Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal(""))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("but has PreferredMaintenanceWindow Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = []byte(`{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when Engine is Aurora", func() {
					BeforeEach(func() {
						rdsProperties2.Engine = "aurora"
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
						Expect(dbCluster.ModifyDBClusterDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				rdsProperties2.PubliclyAccessible = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.PubliclyAccessible).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageEncrypted", func() {
			BeforeEach(func() {
				rdsProperties2.StorageEncrypted = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.StorageEncrypted).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.StorageEncrypted).To(BeFalse())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				rdsProperties2.StorageType = "test-storage-type"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.StorageType).To(Equal("test-storage-type"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbInstance.ModifyDBInstanceDetails.StorageType).To(Equal(""))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				rdsProperties2.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbInstance.ModifyDBInstanceDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when Engine is Aurora", func() {
				BeforeEach(func() {
					rdsProperties2.Engine = "aurora"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
					Expect(dbInstance.ModifyDBInstanceDetails.VpcSecurityGroupIds).To(BeNil())
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				asyncAllowed = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				updateDetails.RawParameters = []byte(`{"backup_retention_period": "invalid"}`)
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("json: cannot unmarshal string into Go value of type int64"))
			})

			Context("and user update parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserUpdateParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				updateDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Plans is not updateable", func() {
			BeforeEach(func() {
				planUpdateable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceNotUpdateable))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				updateDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when modifying the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.ModifyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.ModifyError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when Engine is Aurora", func() {
			BeforeEach(func() {
				rdsProperties2.Engine = "aurora"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
				Expect(dbCluster.ModifyCalled).To(BeTrue())
				Expect(dbCluster.ModifyID).To(Equal(dbClusterIdentifier))
				Expect(dbCluster.ModifyDBClusterDetails.Engine).To(Equal("aurora"))
				Expect(dbCluster.ModifyDBClusterDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
				Expect(dbCluster.ModifyDBClusterDetails.Tags["Updated by"]).To(Equal("AWS RDS Service Broker"))
				Expect(dbCluster.ModifyDBClusterDetails.Tags).To(HaveKey("Updated at"))
				Expect(dbCluster.ModifyDBClusterDetails.Tags["Service ID"]).To(Equal("Service-2"))
				Expect(dbCluster.ModifyDBClusterDetails.Tags["Plan ID"]).To(Equal("Plan-2"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("wehn has ServiceBrokerID set", func() {
				BeforeEach(func() {
					serviceBrokerID = "AwsomeID"
				})

				It("Is setting right Owner value", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.Tags["Owner"]).To(Equal("AwsomeID"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has DBClusterParameterGroupName", func() {
				BeforeEach(func() {
					rdsProperties2.DBClusterParameterGroupName = "test-db-cluster-parameter-group-name"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(context, instanceID, updateDetails, asyncAllowed)
					Expect(dbCluster.ModifyDBClusterDetails.DBClusterParameterGroupName).To(Equal("test-db-cluster-parameter-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})
	})

	var _ = Describe("BulkUpdate", func() {
		var modify func(string, ServiceDetails) error

		BeforeEach(func() {
			modify = func(instanceID string, t ServiceDetails) error {
				return nil
			}

			t := map[string]string{
				"Owner":           "Cloud Foundry",
				"Foo":             "BAR",
				"Plan ID":         "plan-id",
				"Service ID":      "service-id",
				"Organization ID": "org-id",
				"Space ID":        "space-id",
			}
			tc := map[string]string{
				"Owner":           "Custom",
				"Foo":             "BAR",
				"Plan ID":         "plan-id",
				"Service ID":      "service-id",
				"Organization ID": "org-id",
				"Space ID":        "space-id",
			}

			dbCluster.ListDBClustersDetails = []awsrds.DBClusterDetails{
				{
					Identifier:     "cf-cluster-1",
					Endpoint:       "endpoint-address",
					Port:           3306,
					DatabaseName:   "test-db",
					MasterUsername: "master-username",
					Tags:           t,
				},
				{
					Identifier:     "cf-cluster-2",
					Endpoint:       "endpoint-address",
					Port:           3306,
					DatabaseName:   "test-db",
					MasterUsername: "master-username",
					Tags:           tc,
				},
				{
					Identifier:     "FOO-cluster-3",
					Endpoint:       "endpoint-address",
					Port:           3306,
					DatabaseName:   "test-db",
					MasterUsername: "master-username",
					Tags:           t,
				},
			}
			dbInstance.ListDBInstancesDetails = []awsrds.DBInstanceDetails{
				{
					Identifier:     "cf-instance-1",
					Address:        "endpoint-address",
					Port:           3306,
					DBName:         "test-db",
					MasterUsername: "master-username",
					Tags:           t,
				},
				{
					Identifier:     "cf-instance-2",
					Address:        "endpoint-address",
					Port:           3306,
					DBName:         "test-db",
					MasterUsername: "master-username",
					Tags:           tc,
				},
				{
					Identifier:     "FOO-instance-3",
					Address:        "endpoint-address",
					Port:           3306,
					DBName:         "test-db",
					MasterUsername: "master-username",
					Tags:           t,
				},
			}
		})

		It("Will call modifyCluster and modifyInstance", func() {
			m := 0
			i := "cluster-1"
			modify := func(instanceID string, t ServiceDetails) error {
				m++
				Expect(instanceID).Should(Equal(i))
				Expect(instanceID).ShouldNot(Equal("cluster-2"))
				Expect(instanceID).ShouldNot(Equal("FOO-cluster-3"))
				Expect(instanceID).ShouldNot(Equal("instance-2"))
				Expect(instanceID).ShouldNot(Equal("FOO-instance-3"))
				Expect(t.OrgID).Should(Equal("org-id"))
				Expect(t.PlanID).Should(Equal("plan-id"))
				Expect(t.ServiceID).Should(Equal("service-id"))
				Expect(t.SpaceID).Should(Equal("space-id"))
				i = "instance-1"
				return nil
			}
			err := rdsBroker.BulkUpdate(context, modify)
			Expect(m).Should(Equal(2))
			Expect(err).ToNot(HaveOccurred())
		})

		Describe("On custom broker id", func() {
			BeforeEach(func() {
				serviceBrokerID = "Custom"
			})

			It("Will return services of custom broker", func() {
				m := 0
				i := "cluster-2"
				modify := func(instanceID string, t ServiceDetails) error {
					m++
					Expect(instanceID).Should(Equal(i))
					Expect(instanceID).ShouldNot(Equal("cluster-1"))
					Expect(instanceID).ShouldNot(Equal("FOO-cluster-3"))
					Expect(instanceID).ShouldNot(Equal("instance-1"))
					Expect(instanceID).ShouldNot(Equal("FOO-instance-3"))
					Expect(t.OrgID).Should(Equal("org-id"))
					Expect(t.PlanID).Should(Equal("plan-id"))
					Expect(t.ServiceID).Should(Equal("service-id"))
					Expect(t.SpaceID).Should(Equal("space-id"))
					i = "instance-2"
					return nil
				}
				err := rdsBroker.BulkUpdate(context, modify)
				Expect(m).Should(Equal(2))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Describe("On Cluster List Error", func() {
			BeforeEach(func() {
				dbCluster.ListError = errors.New("some-failure")
			})

			It("Will return error", func() {
				err := rdsBroker.BulkUpdate(context, modify)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("On Instance List Error", func() {
			BeforeEach(func() {
				dbInstance.ListError = errors.New("some-failure")
			})

			It("Will return error", func() {
				err := rdsBroker.BulkUpdate(context, modify)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("On Broken modify return Error", func() {
			BeforeEach(func() {
				modify = func(instanceID string, t ServiceDetails) error {
					return errors.New("Super error")
				}
			})

			It("Will return error", func() {
				err := rdsBroker.BulkUpdate(context, modify)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	var _ = Describe("Deprovision", func() {
		var (
			DeprovisionDetails brokerapi.DeprovisionDetails
			asyncAllowed       bool
		)

		BeforeEach(func() {
			DeprovisionDetails = brokerapi.DeprovisionDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}
			asyncAllowed = true
		})

		It("returns the proper response", func() {
			resp, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
			Expect(resp.IsAsync).To(BeTrue())
			Expect(resp.OperationData).To(Equal("Successfull deprovisioned Instance"))
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
			Expect(dbInstance.DeleteCalled).To(BeTrue())
			Expect(dbInstance.DeleteID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.DeleteSkipFinalSnapshot).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when it does not skip final snaphot", func() {
			BeforeEach(func() {
				rdsProperties1.SkipFinalSnapshot = false
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
				Expect(dbInstance.DeleteCalled).To(BeTrue())
				Expect(dbInstance.DeleteID).To(Equal(dbInstanceIdentifier))
				Expect(dbInstance.DeleteSkipFinalSnapshot).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				asyncAllowed = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				DeprovisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when deleting the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DeleteError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DeleteError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when Engine is Aurora", func() {
			BeforeEach(func() {
				rdsProperties1.Engine = "aurora"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
				Expect(dbCluster.DeleteCalled).To(BeTrue())
				Expect(dbCluster.DeleteID).To(Equal(dbClusterIdentifier))
				Expect(dbCluster.DeleteSkipFinalSnapshot).To(BeTrue())
				Expect(dbInstance.DeleteSkipFinalSnapshot).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when it does not skip final snaphot", func() {
				BeforeEach(func() {
					rdsProperties1.SkipFinalSnapshot = false
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
					Expect(dbCluster.DeleteSkipFinalSnapshot).To(BeFalse())
					Expect(dbInstance.DeleteSkipFinalSnapshot).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when deleting the DB Instance fails", func() {
				BeforeEach(func() {
					dbCluster.DeleteError = errors.New("operation failed")
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Deprovision(context, instanceID, DeprovisionDetails, asyncAllowed)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})
	})

	var _ = Describe("Bind", func() {
		var (
			bindDetails brokerapi.BindDetails
		)

		BeforeEach(func() {
			bindDetails = brokerapi.BindDetails{
				ServiceID:     "Service-1",
				PlanID:        "Plan-1",
				AppGUID:       "Application-1",
				RawParameters: []byte(`{}`),
			}

			dbInstance.DescribeDBInstanceDetails = awsrds.DBInstanceDetails{
				Identifier:     dbInstanceIdentifier,
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
			}

			dbCluster.DescribeDBClusterDetails = awsrds.DBClusterDetails{
				Identifier:     dbClusterIdentifier,
				Endpoint:       "endpoint-address",
				Port:           3306,
				DatabaseName:   "test-db",
				MasterUsername: "master-username",
			}
		})

		It("Deals with empty Parameters", func() {
			bindDetails.RawParameters = []byte{}
			_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns the proper response", func() {
			bindingResponse, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
			credentials := bindingResponse.Credentials.(*CredentialsHash)
			Expect(bindingResponse.SyslogDrainURL).To(BeEmpty())
			Expect(credentials.Host).To(Equal("endpoint-address"))
			Expect(credentials.Port).To(Equal(int64(3306)))
			Expect(credentials.Name).To(Equal("test-db"))
			Expect(credentials.Username).To(Equal(dbUsername))
			Expect(credentials.Password).ToNot(BeEmpty())
			Expect(credentials.URI).To(ContainSubstring("@endpoint-address:3306/test-db?reconnect=true"))
			Expect(credentials.JDBCURI).To(ContainSubstring("jdbc:fake://endpoint-address:3306/test-db?user=" + dbUsername + "&password="))
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
			Expect(dbCluster.DescribeCalled).To(BeFalse())
			Expect(dbInstance.DescribeCalled).To(BeTrue())
			Expect(dbInstance.DescribeID).To(Equal(dbInstanceIdentifier))
			Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
			Expect(sqlProvider.GetSQLEngineEngine).To(Equal("test-engine-1"))
			Expect(sqlEngine.OpenCalled).To(BeTrue())
			Expect(sqlEngine.OpenAddress).To(Equal("endpoint-address"))
			Expect(sqlEngine.OpenPort).To(Equal(int64(3306)))
			Expect(sqlEngine.OpenDBName).To(Equal("test-db"))
			Expect(sqlEngine.OpenUsername).To(Equal("master-username"))
			Expect(sqlEngine.OpenPassword).ToNot(BeEmpty())
			Expect(sqlEngine.CreateDBCalled).To(BeFalse())
			Expect(sqlEngine.CreateUserCalled).To(BeTrue())
			Expect(sqlEngine.CreateUserUsername).To(Equal(dbUsername))
			Expect(sqlEngine.CreateUserPassword).ToNot(BeEmpty())
			Expect(sqlEngine.GrantPrivilegesCalled).To(BeTrue())
			Expect(sqlEngine.GrantPrivilegesDBName).To(Equal("test-db"))
			Expect(sqlEngine.GrantPrivilegesUsername).To(Equal(dbUsername))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				bindDetails.RawParameters = []byte(`{"dbname": true}`)
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("json: cannot unmarshal bool into Go value of type string"))
			})

			Context("and user bind parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserBindParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				bindDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Service is not bindable", func() {
			BeforeEach(func() {
				serviceBindable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceNotBindable))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				bindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when Engine is aurora", func() {
			BeforeEach(func() {
				rdsProperties1.Engine = "aurora"
			})

			It("does not describe the DB Instance", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstance.DescribeCalled).To(BeFalse())
			})

			Context("when describing the DB Cluster fails", func() {
				BeforeEach(func() {
					dbCluster.DescribeError = errors.New("operation failed")
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("operation failed"))
				})

				Context("when the DB Cluster does not exists", func() {
					BeforeEach(func() {
						dbCluster.DescribeError = awsrds.ErrDBInstanceDoesNotExist
					})

					It("returns the proper error", func() {
						_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
						Expect(err).To(HaveOccurred())
						Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
					})
				})
			})
		})

		Context("when getting the SQL Engine fails", func() {
			BeforeEach(func() {
				sqlProvider.GetSQLEngineError = errors.New("Engine 'unknown' not supported")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when DBNname Parameter is set", func() {
			BeforeEach(func() {
				bindDetails.RawParameters = []byte(`{"dbname": "my-test-db"}`)
			})

			It("returns the proper response", func() {
				bindingResponse, _ := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				credentials := bindingResponse.Credentials.(*CredentialsHash)
				Expect(bindingResponse.SyslogDrainURL).To(BeEmpty())
				Expect(credentials.Name).To(Equal("my-test-db"))
			})

			It("creates the DB with the proper name", func() {
				rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(sqlEngine.CreateDBCalled).To(BeTrue())
				Expect(sqlEngine.CreateDBDBName).To(Equal("my-test-db"))
				Expect(sqlEngine.GrantPrivilegesDBName).To(Equal("my-test-db"))
			})

			Context("when creating the DB fails", func() {
				BeforeEach(func() {
					sqlEngine.CreateDBError = errors.New("Failed to create sqlEngine")
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("Failed to create sqlEngine"))
					Expect(sqlEngine.CloseCalled).To(BeTrue())
				})
			})
		})

		Context("when creating a DB user fails", func() {
			BeforeEach(func() {
				sqlEngine.CreateUserError = errors.New("Failed to create user")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to create user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})

		Context("when granting privileges fails", func() {
			BeforeEach(func() {
				sqlEngine.GrantPrivilegesError = errors.New("Failed to grant privileges")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(context, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to grant privileges"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})
	})

	var _ = Describe("Unbind", func() {
		var (
			UnbindDetails brokerapi.UnbindDetails
		)

		BeforeEach(func() {
			UnbindDetails = brokerapi.UnbindDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}

			dbInstance.DescribeDBInstanceDetails = awsrds.DBInstanceDetails{
				Identifier:     dbInstanceIdentifier,
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
			}

			dbCluster.DescribeDBClusterDetails = awsrds.DBClusterDetails{
				Identifier:     dbClusterIdentifier,
				Endpoint:       "endpoint-address",
				Port:           3306,
				DatabaseName:   "test-db",
				MasterUsername: "master-username",
			}
		})

		It("makes the proper calls", func() {
			err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
			Expect(dbCluster.DescribeCalled).To(BeFalse())
			Expect(dbInstance.DescribeCalled).To(BeTrue())
			Expect(dbInstance.DescribeID).To(Equal(dbInstanceIdentifier))
			Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
			Expect(sqlProvider.GetSQLEngineEngine).To(Equal("test-engine-1"))
			Expect(sqlEngine.OpenCalled).To(BeTrue())
			Expect(sqlEngine.OpenAddress).To(Equal("endpoint-address"))
			Expect(sqlEngine.OpenPort).To(Equal(int64(3306)))
			Expect(sqlEngine.OpenDBName).To(Equal("test-db"))
			Expect(sqlEngine.OpenUsername).To(Equal("master-username"))
			Expect(sqlEngine.OpenPassword).ToNot(BeEmpty())
			Expect(sqlEngine.PrivilegesCalled).To(BeTrue())
			Expect(sqlEngine.RevokePrivilegesCalled).To(BeFalse())
			Expect(sqlEngine.DropDBCalled).To(BeFalse())
			Expect(sqlEngine.DropUserCalled).To(BeTrue())
			Expect(sqlEngine.DropUserUsername).To(Equal(dbUsername))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				UnbindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when Engine is aurora", func() {
			BeforeEach(func() {
				rdsProperties1.Engine = "aurora"
			})

			It("does not describe the DB Instance", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstance.DescribeCalled).To(BeFalse())
			})

			Context("when describing the DB Cluster fails", func() {
				BeforeEach(func() {
					dbCluster.DescribeError = errors.New("operation failed")
				})

				It("returns the proper error", func() {
					err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("operation failed"))
				})

				Context("when the DB Cluster does not exists", func() {
					BeforeEach(func() {
						dbCluster.DescribeError = awsrds.ErrDBInstanceDoesNotExist
					})

					It("returns the proper error", func() {
						err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
						Expect(err).To(HaveOccurred())
						Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
					})
				})
			})
		})

		Context("when getting the SQL Engine fails", func() {
			BeforeEach(func() {
				sqlProvider.GetSQLEngineError = errors.New("SQL Engine 'unknown' not supported")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("SQL Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when getting privileges fails", func() {
			BeforeEach(func() {
				sqlEngine.PrivilegesError = errors.New("Failed to get privileges")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to get privileges"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})

		Context("when user has privileges over a DB", func() {
			BeforeEach(func() {
				sqlEngine.PrivilegesPrivileges = map[string][]string{"test-db": []string{dbUsername}}
			})

			It("makes the proper calls", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(sqlEngine.RevokePrivilegesCalled).To(BeTrue())
				Expect(sqlEngine.RevokePrivilegesDBName).To(Equal("test-db"))
				Expect(sqlEngine.RevokePrivilegesUsername).To(Equal(dbUsername))
				Expect(sqlEngine.DropDBCalled).To(BeFalse())
				Expect(sqlEngine.CloseCalled).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when revoking privileges fails", func() {
				BeforeEach(func() {
					sqlEngine.RevokePrivilegesError = errors.New("Failed to revoke privileges")
				})

				It("returns the proper error", func() {
					err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("Failed to revoke privileges"))
					Expect(sqlEngine.CloseCalled).To(BeTrue())
				})
			})

			Context("and the db is not the master db", func() {
				BeforeEach(func() {
					sqlEngine.PrivilegesPrivileges = map[string][]string{"another-test-db": []string{dbUsername}}
				})

				It("makes the proper calls", func() {
					err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
					Expect(sqlEngine.RevokePrivilegesCalled).To(BeTrue())
					Expect(sqlEngine.RevokePrivilegesDBName).To(Equal("another-test-db"))
					Expect(sqlEngine.RevokePrivilegesUsername).To(Equal(dbUsername))
					Expect(sqlEngine.DropDBCalled).To(BeTrue())
					Expect(sqlEngine.DropDBDBName).To(Equal("another-test-db"))
					Expect(sqlEngine.CloseCalled).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when droping the DB fails", func() {
					BeforeEach(func() {
						sqlEngine.DropDBError = errors.New("Failed to drop db")
					})

					It("returns the proper error", func() {
						err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to drop db"))
						Expect(sqlEngine.CloseCalled).To(BeTrue())
					})
				})

				Context("but there are other users with grants over the db", func() {
					BeforeEach(func() {
						sqlEngine.PrivilegesPrivileges = map[string][]string{"another-test-db": []string{dbUsername, "another-user"}}
					})

					It("makes the proper calls", func() {
						err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
						Expect(sqlEngine.RevokePrivilegesCalled).To(BeTrue())
						Expect(sqlEngine.RevokePrivilegesDBName).To(Equal("another-test-db"))
						Expect(sqlEngine.RevokePrivilegesUsername).To(Equal(dbUsername))
						Expect(sqlEngine.DropDBCalled).To(BeFalse())
						Expect(sqlEngine.CloseCalled).To(BeTrue())
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when deleting a user fails", func() {
			BeforeEach(func() {
				sqlEngine.DropUserError = errors.New("Failed to delete user")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(context, instanceID, bindingID, UnbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to delete user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})
	})

	var _ = Describe("LastOperation", func() {
		var (
			dbInstanceStatus            string
			lastOperationState          brokerapi.LastOperationState
			properLastOperationResponse brokerapi.LastOperation
		)

		JustBeforeEach(func() {
			dbInstance.DescribeDBInstanceDetails = awsrds.DBInstanceDetails{
				Identifier:     dbInstanceIdentifier,
				Engine:         "test-engine",
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
				Status:         dbInstanceStatus,
			}

			properLastOperationResponse = brokerapi.LastOperation{
				State:       lastOperationState,
				Description: "DB Instance '" + dbInstanceIdentifier + "' status is '" + dbInstanceStatus + "'",
			}
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.LastOperation(context, instanceID, "")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.LastOperation(context, instanceID, "")
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when last operation is still in progress", func() {
			BeforeEach(func() {
				dbInstanceStatus = "creating"
				lastOperationState = brokerapi.InProgress
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(context, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "failed"
				lastOperationState = brokerapi.Failed
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(context, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation succeeded", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				lastOperationState = brokerapi.Succeeded
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(context, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("but has pending modifications", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.PendingModifications = true

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending modifications",
					}
				})

				It("returns the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(context, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})
		})
	})
})
