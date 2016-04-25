package awsrds_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/awsrds"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("RDS DB Instance", func() {
	var (
		region               string
		dbInstanceIdentifier string

		awsSession *session.Session

		iamsvc  *iam.IAM
		iamCall func(r *request.Request)

		rdssvc  *rds.RDS
		rdsCall func(r *request.Request)

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsDBInstance DBInstance
	)

	BeforeEach(func() {
		region = "rds-region"
		dbInstanceIdentifier = "cf-instance-id"
	})

	JustBeforeEach(func() {
		awsSession = session.New(nil)

		iamsvc = iam.New(awsSession)
		rdssvc = rds.New(awsSession)

		logger = lager.NewLogger("rdsdbinstance_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsDBInstance = NewRDSDBInstance(region, iamsvc, rdssvc, logger)
	})

	var _ = Describe("Describe", func() {
		var (
			properDBInstanceDetails DBInstanceDetails

			describeDBInstances []*rds.DBInstance
			describeDBInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error
		)

		BeforeEach(func() {
			properDBInstanceDetails = DBInstanceDetails{
				Identifier:       dbInstanceIdentifier,
				Status:           "available",
				Engine:           "test-engine",
				EngineVersion:    "1.2.3",
				DBName:           "test-dbname",
				MasterUsername:   "test-master-username",
				AllocatedStorage: int64(100),
			}

			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceStatus:     aws.String("available"),
				Engine:               aws.String("test-engine"),
				EngineVersion:        aws.String("1.2.3"),
				DBName:               aws.String("test-dbname"),
				MasterUsername:       aws.String("test-master-username"),
				AllocatedStorage:     aws.Int64(100),
			}
			describeDBInstances = []*rds.DBInstance{describeDBInstance}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}
			describeDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
				Expect(r.Params).To(Equal(describeDBInstancesInput))
				data := r.Data.(*rds.DescribeDBInstancesOutput)
				data.DBInstances = describeDBInstances
				r.Error = describeDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper DB Instance", func() {
			dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
		})

		Context("when RDS DB Instance has an Endpoint", func() {
			BeforeEach(func() {
				describeDBInstance.Endpoint = &rds.Endpoint{
					Address: aws.String("dbinstance-endpoint"),
					Port:    aws.Int64(3306),
				}

				properDBInstanceDetails.Address = "dbinstance-endpoint"
				properDBInstanceDetails.Port = int64(3306)
			})

			It("returns the proper DB Instance", func() {
				dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
			})
		})

		Context("when RDS DB Instance has pending modifications", func() {
			BeforeEach(func() {
				describeDBInstance.PendingModifiedValues = &rds.PendingModifiedValues{
					DBInstanceClass: aws.String("new-instance-class"),
				}
				properDBInstanceDetails.PendingModifications = true
			})

			It("returns the proper DB Instance", func() {
				dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
			})
		})

		Context("when the DB instance does not exists", func() {
			JustBeforeEach(func() {
				describeDBInstancesInput = &rds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("unknown"),
				}
			})

			It("returns the proper error", func() {
				_, err := rdsDBInstance.Describe("unknown")
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					describeDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					describeDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Create", func() {
		var (
			dbInstanceDetails DBInstanceDetails

			createDBInstanceInput *rds.CreateDBInstanceInput
			createDBInstanceError error
		)

		BeforeEach(func() {
			dbInstanceDetails = DBInstanceDetails{
				Engine: "test-engine",
			}

			createDBInstanceInput = &rds.CreateDBInstanceInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				Engine:                  aws.String("test-engine"),
				AutoMinorVersionUpgrade: aws.Bool(false),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
				PubliclyAccessible:      aws.Bool(false),
				StorageEncrypted:        aws.Bool(false),
			}
			createDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateDBInstance"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.CreateDBInstanceInput{}))
				Expect(r.Params).To(Equal(createDBInstanceInput))
				r.Error = createDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				dbInstanceDetails.AllocatedStorage = 100
				createDBInstanceInput.AllocatedStorage = aws.Int64(100)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				dbInstanceDetails.AutoMinorVersionUpgrade = true
				createDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				dbInstanceDetails.AvailabilityZone = "test-az"
				createDBInstanceInput.AvailabilityZone = aws.String("test-az")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				dbInstanceDetails.BackupRetentionPeriod = 7
				createDBInstanceInput.BackupRetentionPeriod = aws.Int64(7)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				dbInstanceDetails.CharacterSetName = "test-characterset-name"
				createDBInstanceInput.CharacterSetName = aws.String("test-characterset-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				dbInstanceDetails.CopyTagsToSnapshot = true
				createDBInstanceInput.CopyTagsToSnapshot = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBClusterIdentifier", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBClusterIdentifier = "test-db-cluster-identifier"
				createDBInstanceInput.DBClusterIdentifier = aws.String("test-db-cluster-identifier")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBInstanceClass", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBInstanceClass = "db.m3.small"
				createDBInstanceInput.DBInstanceClass = aws.String("db.m3.small")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBName = "test-dbname"
				createDBInstanceInput.DBName = aws.String("test-dbname")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBParameterGroupName = "test-db-parameter-group-name"
				createDBInstanceInput.DBParameterGroupName = aws.String("test-db-parameter-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSecurityGroups = []string{"test-db-security-group"}
				createDBInstanceInput.DBSecurityGroups = aws.StringSlice([]string{"test-db-security-group"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSubnetGroupName = "test-db-subnet-group-name"
				createDBInstanceInput.DBSubnetGroupName = aws.String("test-db-subnet-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				dbInstanceDetails.EngineVersion = "1.2.3"
				createDBInstanceInput.EngineVersion = aws.String("1.2.3")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has KmsKeyID", func() {
			BeforeEach(func() {
				dbInstanceDetails.KmsKeyID = "test-kms-key-id"
				createDBInstanceInput.KmsKeyId = aws.String("test-kms-key-id")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUsername", func() {
			BeforeEach(func() {
				dbInstanceDetails.MasterUsername = "test-master-username"
				createDBInstanceInput.MasterUsername = aws.String("test-master-username")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUserPassword", func() {
			BeforeEach(func() {
				dbInstanceDetails.MasterUserPassword = "test-master-user-password"
				createDBInstanceInput.MasterUserPassword = aws.String("test-master-user-password")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				dbInstanceDetails.LicenseModel = "test-license-model"
				createDBInstanceInput.LicenseModel = aws.String("test-license-model")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				dbInstanceDetails.MultiAZ = true
				createDBInstanceInput.MultiAZ = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.OptionGroupName = "test-option-group-name"
				createDBInstanceInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				dbInstanceDetails.Port = 666
				createDBInstanceInput.Port = aws.Int64(666)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredBackupWindow = "test-preferred-backup-window"
				createDBInstanceInput.PreferredBackupWindow = aws.String("test-preferred-backup-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				createDBInstanceInput.PreferredMaintenanceWindow = aws.String("test-preferred-maintenance-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				dbInstanceDetails.PubliclyAccessible = true
				createDBInstanceInput.PubliclyAccessible = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageEncrypted", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageEncrypted = true
				createDBInstanceInput.StorageEncrypted = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageType = "test-storage-type"
				createDBInstanceInput.StorageType = aws.String("test-storage-type")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				dbInstanceDetails.Iops = 1000
				createDBInstanceInput.Iops = aws.Int64(1000)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				dbInstanceDetails.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				createDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice([]string{"test-vpc-security-group-ids"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbInstanceDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
				createDBInstanceInput.Tags = []*rds.Tag{
					&rds.Tag{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")},
				}
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when creating the DB Instance fails", func() {
			BeforeEach(func() {
				createDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Modify", func() {
		var (
			dbInstanceDetails DBInstanceDetails
			applyImmediately  bool

			describeDBInstances []*rds.DBInstance
			describeDBInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error

			modifyDBInstanceInput *rds.ModifyDBInstanceInput
			modifyDBInstanceError error

			addTagsToResourceInput *rds.AddTagsToResourceInput
			addTagsToResourceError error

			user         *iam.User
			getUserInput *iam.GetUserInput
			getUserError error
		)

		BeforeEach(func() {
			dbInstanceDetails = DBInstanceDetails{}
			applyImmediately = false

			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceStatus:     aws.String("available"),
				Engine:               aws.String("test-engine"),
				EngineVersion:        aws.String("1.2.3"),
				DBName:               aws.String("test-dbname"),
				MasterUsername:       aws.String("test-master-username"),
				AllocatedStorage:     aws.Int64(100),
			}
			describeDBInstances = []*rds.DBInstance{describeDBInstance}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}
			describeDBInstanceError = nil

			modifyDBInstanceInput = &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				ApplyImmediately:        aws.Bool(applyImmediately),
				AutoMinorVersionUpgrade: aws.Bool(false),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
			}
			modifyDBInstanceError = nil

			addTagsToResourceInput = &rds.AddTagsToResourceInput{
				ResourceName: aws.String("arn:aws:rds:rds-region:account:db:" + dbInstanceIdentifier),
				Tags: []*rds.Tag{
					&rds.Tag{
						Key:   aws.String("Owner"),
						Value: aws.String("Cloud Foundry"),
					},
				},
			}
			addTagsToResourceError = nil

			user = &iam.User{
				Arn: aws.String("arn:aws:service:region:account:resource"),
			}
			getUserInput = &iam.GetUserInput{}
			getUserError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ModifyDBInstance|AddTagsToResource"))
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					Expect(r.Params).To(Equal(describeDBInstancesInput))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = describeDBInstances
					r.Error = describeDBInstanceError
				case "ModifyDBInstance":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ModifyDBInstanceInput{}))
					Expect(r.Params).To(Equal(modifyDBInstanceInput))
					r.Error = modifyDBInstanceError
				case "AddTagsToResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.AddTagsToResourceInput{}))
					Expect(r.Params).To(Equal(addTagsToResourceInput))
					r.Error = addTagsToResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)

			iamsvc.Handlers.Clear()
			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("GetUser"))
				Expect(r.Params).To(Equal(getUserInput))
				data := r.Data.(*iam.GetUserOutput)
				data.User = user
				r.Error = getUserError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when apply immediately is set to true", func() {
			BeforeEach(func() {
				applyImmediately = true
				modifyDBInstanceInput.ApplyImmediately = aws.Bool(true)
			})

			It("returns the proper DB Instance", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when is a different DB engine", func() {
			BeforeEach(func() {
				dbInstanceDetails.Engine = "new-engine"
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Migrating the RDS DB Instance engine from 'test-engine' to 'new-engine' is not supported"))
			})
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				dbInstanceDetails.AllocatedStorage = 500
				modifyDBInstanceInput.AllocatedStorage = aws.Int64(500)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("and new value is less than old value", func() {
				BeforeEach(func() {
					dbInstanceDetails.AllocatedStorage = 50
					modifyDBInstanceInput.AllocatedStorage = aws.Int64(100)
				})

				It("picks up the old value", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				dbInstanceDetails.AutoMinorVersionUpgrade = true
				modifyDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				dbInstanceDetails.BackupRetentionPeriod = 7
				modifyDBInstanceInput.BackupRetentionPeriod = aws.Int64(7)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				dbInstanceDetails.CopyTagsToSnapshot = true
				modifyDBInstanceInput.CopyTagsToSnapshot = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBInstanceClass", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBInstanceClass = "db.m3.small"
				modifyDBInstanceInput.DBInstanceClass = aws.String("db.m3.small")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBParameterGroupName = "test-db-parameter-group-name"
				modifyDBInstanceInput.DBParameterGroupName = aws.String("test-db-parameter-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSecurityGroups = []string{"test-db-security-group"}
				modifyDBInstanceInput.DBSecurityGroups = aws.StringSlice([]string{"test-db-security-group"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				dbInstanceDetails.EngineVersion = "1.2.4"
				modifyDBInstanceInput.EngineVersion = aws.String("1.2.4")
				modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(false)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("and is a major version upgrade", func() {
				BeforeEach(func() {
					dbInstanceDetails.EngineVersion = "1.3.3"
					modifyDBInstanceInput.EngineVersion = aws.String("1.3.3")
					modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(true)
				})

				It("does not return error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				dbInstanceDetails.MultiAZ = true
				modifyDBInstanceInput.MultiAZ = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.OptionGroupName = "test-option-group-name"
				modifyDBInstanceInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredBackupWindow = "test-preferred-backup-window"
				modifyDBInstanceInput.PreferredBackupWindow = aws.String("test-preferred-backup-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				modifyDBInstanceInput.PreferredMaintenanceWindow = aws.String("test-preferred-maintenance-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageType = "test-storage-type"
				modifyDBInstanceInput.StorageType = aws.String("test-storage-type")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				dbInstanceDetails.Iops = 1000
				modifyDBInstanceInput.Iops = aws.Int64(1000)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				dbInstanceDetails.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				modifyDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice([]string{"test-vpc-security-group-ids"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbInstanceDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when adding tags to resource fails", func() {
				BeforeEach(func() {
					addTagsToResourceError = errors.New("operation failed")
				})

				It("does not return error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when getting user arn fails", func() {
				BeforeEach(func() {
					getUserError = errors.New("operation failed")
				})

				It("does not return error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when modifying the DB instance fails", func() {
			BeforeEach(func() {
				modifyDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					modifyDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Delete", func() {
		var (
			skipFinalSnapshot         bool
			finalDBSnapshotIdentifier string

			deleteDBInstanceError error
		)

		BeforeEach(func() {
			skipFinalSnapshot = true
			finalDBSnapshotIdentifier = ""
			deleteDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeleteDBInstance"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.DeleteDBInstanceInput{}))
				params := r.Params.(*rds.DeleteDBInstanceInput)
				Expect(params.DBInstanceIdentifier).To(Equal(aws.String(dbInstanceIdentifier)))
				if finalDBSnapshotIdentifier != "" {
					Expect(*params.FinalDBSnapshotIdentifier).To(ContainSubstring(finalDBSnapshotIdentifier))
				} else {
					Expect(params.FinalDBSnapshotIdentifier).To(BeNil())
				}
				Expect(params.SkipFinalSnapshot).To(Equal(aws.Bool(skipFinalSnapshot)))
				r.Error = deleteDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when does not skip the final snapshot", func() {
			BeforeEach(func() {
				skipFinalSnapshot = false
				finalDBSnapshotIdentifier = "rds-broker-" + dbInstanceIdentifier
			})

			It("returns the proper DB Instance", func() {
				err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when deleting the DB instance fails", func() {
			BeforeEach(func() {
				deleteDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deleteDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					deleteDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
				})
			})
		})
	})
})
