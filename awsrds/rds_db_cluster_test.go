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

var _ = Describe("RDS DB Cluster", func() {
	var (
		region              string
		dbClusterIdentifier string

		awsSession *session.Session

		iamsvc  *iam.IAM
		iamCall func(r *request.Request)

		rdssvc  *rds.RDS
		rdsCall func(r *request.Request)

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsDBCluster DBCluster
	)

	BeforeEach(func() {
		region = "rds-region"
		dbClusterIdentifier = "cf-cluster-id"
	})

	JustBeforeEach(func() {
		awsSession = session.New(nil)

		iamsvc = iam.New(awsSession)
		rdssvc = rds.New(awsSession)

		logger = lager.NewLogger("rdsdbcluster_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsDBCluster = NewRDSDBCluster(region, iamsvc, rdssvc, logger)
	})

	var _ = Describe("Describe", func() {
		var (
			properDBClusterDetails DBClusterDetails

			describeDBClusters []*rds.DBCluster
			describeDBCluster  *rds.DBCluster

			describeDBClustersInput *rds.DescribeDBClustersInput
			describeDBClusterError  error
		)

		BeforeEach(func() {
			properDBClusterDetails = DBClusterDetails{
				Identifier:       dbClusterIdentifier,
				Status:           "available",
				Engine:           "test-engine",
				EngineVersion:    "1.2.3",
				DatabaseName:     "test-dbname",
				MasterUsername:   "test-master-username",
				AllocatedStorage: int64(100),
				Endpoint:         "test-endpoint",
				Port:             int64(3306),
			}

			describeDBCluster = &rds.DBCluster{
				DBClusterIdentifier: aws.String(dbClusterIdentifier),
				Status:              aws.String("available"),
				Engine:              aws.String("test-engine"),
				EngineVersion:       aws.String("1.2.3"),
				DatabaseName:        aws.String("test-dbname"),
				MasterUsername:      aws.String("test-master-username"),
				AllocatedStorage:    aws.Int64(100),
				Endpoint:            aws.String("test-endpoint"),
				Port:                aws.Int64(3306),
			}
			describeDBClusters = []*rds.DBCluster{describeDBCluster}

			describeDBClustersInput = &rds.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String(dbClusterIdentifier),
			}
			describeDBClusterError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBClusters"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBClustersInput{}))
				Expect(r.Params).To(Equal(describeDBClustersInput))
				data := r.Data.(*rds.DescribeDBClustersOutput)
				data.DBClusters = describeDBClusters
				r.Error = describeDBClusterError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper DB Cluster", func() {
			dbClusterDetails, err := rdsDBCluster.Describe(dbClusterIdentifier)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbClusterDetails).To(Equal(properDBClusterDetails))
		})

		Context("when the DB Cluster does not exists", func() {
			JustBeforeEach(func() {
				describeDBClustersInput = &rds.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("unknown"),
				}
			})

			It("returns the proper error", func() {
				_, err := rdsDBCluster.Describe("unknown")
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ErrDBClusterDoesNotExist))
			})
		})

		Context("when describing the DB Cluster fails", func() {
			BeforeEach(func() {
				describeDBClusterError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsDBCluster.Describe(dbClusterIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					describeDBClusterError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsDBCluster.Describe(dbClusterIdentifier)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					describeDBClusterError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					_, err := rdsDBCluster.Describe(dbClusterIdentifier)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBClusterDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Create", func() {
		var (
			dbClusterDetails DBClusterDetails

			createDBClustersInput *rds.CreateDBClusterInput
			createDBClusterError  error
		)

		BeforeEach(func() {
			dbClusterDetails = DBClusterDetails{
				Engine: "test-engine",
			}

			createDBClustersInput = &rds.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(dbClusterIdentifier),
				Engine:              aws.String("test-engine"),
			}
			createDBClusterError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateDBCluster"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.CreateDBClusterInput{}))
				Expect(r.Params).To(Equal(createDBClustersInput))
				r.Error = createDBClusterError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when has AvailabilityZones", func() {
			BeforeEach(func() {
				dbClusterDetails.AvailabilityZones = []string{"test-az"}
				createDBClustersInput.AvailabilityZones = aws.StringSlice([]string{"test-az"})
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				dbClusterDetails.BackupRetentionPeriod = 7
				createDBClustersInput.BackupRetentionPeriod = aws.Int64(7)
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				dbClusterDetails.CharacterSetName = "test-characterset-name"
				createDBClustersInput.CharacterSetName = aws.String("test-characterset-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DatabaseName", func() {
			BeforeEach(func() {
				dbClusterDetails.DatabaseName = "test-database-name"
				createDBClustersInput.DatabaseName = aws.String("test-database-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBClusterParameterGroupName", func() {
			BeforeEach(func() {
				dbClusterDetails.DBClusterParameterGroupName = "test-db-cluster-parameter-group-name"
				createDBClustersInput.DBClusterParameterGroupName = aws.String("test-db-cluster-parameter-group-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				dbClusterDetails.DBSubnetGroupName = "test-db-subnet-group-name"
				createDBClustersInput.DBSubnetGroupName = aws.String("test-db-subnet-group-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				dbClusterDetails.EngineVersion = "1.2.3"
				createDBClustersInput.EngineVersion = aws.String("1.2.3")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUsername", func() {
			BeforeEach(func() {
				dbClusterDetails.MasterUsername = "test-master-username"
				createDBClustersInput.MasterUsername = aws.String("test-master-username")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUserPassword", func() {
			BeforeEach(func() {
				dbClusterDetails.MasterUserPassword = "test-master-user-password"
				createDBClustersInput.MasterUserPassword = aws.String("test-master-user-password")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbClusterDetails.OptionGroupName = "test-option-group-name"
				createDBClustersInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				dbClusterDetails.Port = 666
				createDBClustersInput.Port = aws.Int64(666)
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				dbClusterDetails.PreferredBackupWindow = "test-preferred-backup-window"
				createDBClustersInput.PreferredBackupWindow = aws.String("test-preferred-backup-window")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				dbClusterDetails.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				createDBClustersInput.PreferredMaintenanceWindow = aws.String("test-preferred-maintenance-window")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				dbClusterDetails.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				createDBClustersInput.VpcSecurityGroupIds = aws.StringSlice([]string{"test-vpc-security-group-ids"})
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbClusterDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
				createDBClustersInput.Tags = []*rds.Tag{
					&rds.Tag{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")},
				}
			})

			It("does not return error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when creating the DB Cluster fails", func() {
			BeforeEach(func() {
				createDBClusterError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createDBClusterError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBCluster.Create(dbClusterIdentifier, dbClusterDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Modify", func() {
		var (
			dbClusterDetails DBClusterDetails
			applyImmediately bool

			describeDBClustersInput *rds.DescribeDBClustersInput
			describeDBClusterError  error

			modifyDBClusterInput *rds.ModifyDBClusterInput
			modifyDBClusterError error

			addTagsToResourceInput *rds.AddTagsToResourceInput
			addTagsToResourceError error

			user         *iam.User
			getUserInput *iam.GetUserInput
			getUserError error
		)

		BeforeEach(func() {
			dbClusterDetails = DBClusterDetails{}
			applyImmediately = false

			describeDBClustersInput = &rds.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String(dbClusterIdentifier),
			}
			describeDBClusterError = nil

			modifyDBClusterInput = &rds.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String(dbClusterIdentifier),
				ApplyImmediately:    aws.Bool(applyImmediately),
			}
			modifyDBClusterError = nil

			addTagsToResourceInput = &rds.AddTagsToResourceInput{
				ResourceName: aws.String("arn:aws:rds:rds-region:account:db:" + dbClusterIdentifier),
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
				Expect(r.Operation.Name).To(MatchRegexp("ModifyDBCluster|AddTagsToResource"))
				switch r.Operation.Name {
				case "ModifyDBCluster":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ModifyDBClusterInput{}))
					Expect(r.Params).To(Equal(modifyDBClusterInput))
					r.Error = modifyDBClusterError
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
			err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when apply immediately is set to true", func() {
			BeforeEach(func() {
				applyImmediately = true
				modifyDBClusterInput.ApplyImmediately = aws.Bool(true)
			})

			It("returns the proper DB Cluster", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				dbClusterDetails.BackupRetentionPeriod = 7
				modifyDBClusterInput.BackupRetentionPeriod = aws.Int64(7)
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBClusterParameterGroupName", func() {
			BeforeEach(func() {
				dbClusterDetails.DBClusterParameterGroupName = "test-db-cluster-parameter-group-name"
				modifyDBClusterInput.DBClusterParameterGroupName = aws.String("test-db-cluster-parameter-group-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUserPassword", func() {
			BeforeEach(func() {
				dbClusterDetails.MasterUserPassword = "test-master-user-password"
				modifyDBClusterInput.MasterUserPassword = aws.String("test-master-user-password")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbClusterDetails.OptionGroupName = "test-option-group-name"
				modifyDBClusterInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				dbClusterDetails.Port = 666
				modifyDBClusterInput.Port = aws.Int64(666)
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				dbClusterDetails.PreferredBackupWindow = "test-preferred-backup-window"
				modifyDBClusterInput.PreferredBackupWindow = aws.String("test-preferred-backup-window")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				dbClusterDetails.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				modifyDBClusterInput.PreferredMaintenanceWindow = aws.String("test-preferred-maintenance-window")
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				dbClusterDetails.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				modifyDBClusterInput.VpcSecurityGroupIds = aws.StringSlice([]string{"test-vpc-security-group-ids"})
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbClusterDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
			})

			It("does not return error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when adding tags to resource fails", func() {
				BeforeEach(func() {
					addTagsToResourceError = errors.New("operation failed")
				})

				It("does not return error", func() {
					err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when getting user arn fails", func() {
				BeforeEach(func() {
					getUserError = errors.New("operation failed")
				})

				It("does not return error", func() {
					err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when modifying the DB cluster fails", func() {
			BeforeEach(func() {
				modifyDBClusterError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					modifyDBClusterError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					modifyDBClusterError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					err := rdsDBCluster.Modify(dbClusterIdentifier, dbClusterDetails, applyImmediately)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBClusterDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Delete", func() {
		var (
			skipFinalSnapshot         bool
			finalDBSnapshotIdentifier string

			deleteDBClusterError error
		)

		BeforeEach(func() {
			skipFinalSnapshot = true
			finalDBSnapshotIdentifier = ""
			deleteDBClusterError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeleteDBCluster"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.DeleteDBClusterInput{}))
				params := r.Params.(*rds.DeleteDBClusterInput)
				Expect(params.DBClusterIdentifier).To(Equal(aws.String(dbClusterIdentifier)))
				if finalDBSnapshotIdentifier != "" {
					Expect(*params.FinalDBSnapshotIdentifier).To(ContainSubstring(finalDBSnapshotIdentifier))
				} else {
					Expect(params.FinalDBSnapshotIdentifier).To(BeNil())
				}
				Expect(params.SkipFinalSnapshot).To(Equal(aws.Bool(skipFinalSnapshot)))
				r.Error = deleteDBClusterError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBCluster.Delete(dbClusterIdentifier, skipFinalSnapshot)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when does not skip the final snapshot", func() {
			BeforeEach(func() {
				skipFinalSnapshot = false
				finalDBSnapshotIdentifier = "rds-broker-" + dbClusterIdentifier
			})

			It("returns the proper DB Cluster", func() {
				err := rdsDBCluster.Delete(dbClusterIdentifier, skipFinalSnapshot)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when deleting the DB Cluster fails", func() {
			BeforeEach(func() {
				deleteDBClusterError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBCluster.Delete(dbClusterIdentifier, skipFinalSnapshot)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deleteDBClusterError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBCluster.Delete(dbClusterIdentifier, skipFinalSnapshot)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					deleteDBClusterError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					err := rdsDBCluster.Delete(dbClusterIdentifier, skipFinalSnapshot)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBClusterDoesNotExist))
				})
			})
		})
	})
})
