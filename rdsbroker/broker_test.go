package rdsbroker_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cf-platform-eng/rds-broker/rdsbroker"

	"github.com/cf-platform-eng/rds-broker/database/fakes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/frodenas/brokerapi"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("RDS Broker", func() {
	var (
		rdsProperties RDSProperties
		plan1         ServicePlan
		plan2         ServicePlan
		service1      Service
		service2      Service
		catalog       Catalog

		config Config

		iamsvc  *iam.IAM
		iamCall func(r *request.Request)

		rdssvc  *rds.RDS
		rdsCall func(r *request.Request)

		dbProvider *fakes.FakeProvider
		database   *fakes.FakeDatabase

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsBroker *RDSBroker

		allowUserProvisionParameters bool
		allowUserUpdateParameters    bool
		allowUserBindParameters      bool
		serviceBindable              bool
		planUpdateable               bool
		skipFinalSnapshot            bool

		instanceID           = "instance-id"
		bindingID            = "binding-id"
		dbInstanceIdentifier = "cf-instance-id"
		dbUsername           = "YmluZGluZy1pZNQd"
	)

	BeforeEach(func() {
		allowUserProvisionParameters = true
		allowUserUpdateParameters = true
		allowUserBindParameters = true
		serviceBindable = true
		planUpdateable = true
		skipFinalSnapshot = true

		dbProvider = &fakes.FakeProvider{}
		database = &fakes.FakeDatabase{}
		dbProvider.GetDatabaseDatabase = database
	})

	JustBeforeEach(func() {
		rdsProperties = RDSProperties{
			DBInstanceClass:   "db.m3.medium",
			Engine:            "mysql",
			EngineVersion:     "5.6.23",
			AllocatedStorage:  100,
			SkipFinalSnapshot: skipFinalSnapshot,
		}

		plan1 = ServicePlan{
			ID:            "Plan-1",
			Name:          "Plan 1",
			Description:   "This is the Plan 1",
			RDSProperties: rdsProperties,
		}
		plan2 = ServicePlan{
			ID:            "Plan-2",
			Name:          "Plan 2",
			Description:   "This is the Plan 2",
			RDSProperties: rdsProperties,
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
			Catalog:                      catalog,
		}

		iamsvc = iam.New(nil)
		rdssvc = rds.New(nil)

		logger = lager.NewLogger("rdsbroker_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsBroker = New(config, iamsvc, rdssvc, dbProvider, logger)
	})

	var _ = Describe("Services", func() {
		var (
			properCatalogResponse brokerapi.CatalogResponse
		)

		BeforeEach(func() {
			properCatalogResponse = brokerapi.CatalogResponse{
				Services: []brokerapi.Service{
					brokerapi.Service{
						ID:             "Service-1",
						Name:           "Service 1",
						Description:    "This is the Service 1",
						Bindable:       serviceBindable,
						PlanUpdateable: planUpdateable,
						Plans: []brokerapi.ServicePlan{
							brokerapi.ServicePlan{
								ID:          "Plan-1",
								Name:        "Plan 1",
								Description: "This is the Plan 1",
							},
						},
					},
					brokerapi.Service{
						ID:             "Service-2",
						Name:           "Service 2",
						Description:    "This is the Service 2",
						Bindable:       serviceBindable,
						PlanUpdateable: planUpdateable,
						Plans: []brokerapi.ServicePlan{
							brokerapi.ServicePlan{
								ID:          "Plan-2",
								Name:        "Plan 2",
								Description: "This is the Plan 2",
							},
						},
					},
				},
			}
		})

		It("returns the proper CatalogResponse", func() {
			brokerCatalog := rdsBroker.Services()
			Expect(brokerCatalog).To(Equal(properCatalogResponse))
		})

	})

	var _ = Describe("Provision", func() {
		var (
			provisionDetails  brokerapi.ProvisionDetails
			acceptsIncomplete bool

			properProvisioningResponse brokerapi.ProvisioningResponse

			createDBInstancesInput *rds.CreateDBInstanceInput
			createDBInstanceCall   func(r *request.Request)
			createDBInstanceError  error
		)

		BeforeEach(func() {
			createDBInstanceError = nil

			provisionDetails = brokerapi.ProvisionDetails{
				OrganizationGUID: "organization-id",
				PlanID:           "Plan-1",
				ServiceID:        "Service-1",
				SpaceGUID:        "space-id",
				Parameters:       map[string]interface{}{},
			}
			acceptsIncomplete = true

			properProvisioningResponse = brokerapi.ProvisioningResponse{}
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			createDBInstancesInput = &rds.CreateDBInstanceInput{}

			createDBInstanceCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateDBInstance"))
				// TODO: Expect(r.Params).To(Equal(createDBInstancesInput))
				r.Error = createDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(createDBInstanceCall)
		})

		It("returns the proper response", func() {
			provisioningResponse, asynch, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
			Expect(provisioningResponse).To(Equal(properProvisioningResponse))
			Expect(asynch).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				provisionDetails.Parameters = map[string]interface{}{"backup_retention_period": "invalid"}
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'backup_retention_period' expected type 'int64', got unconvertible type 'string'"))
			})

			Context("and user provision parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserProvisionParameters = false
				})

				It("does not return an error", func() {
					_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				provisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when creating the DB instance fails", func() {
			BeforeEach(func() {
				createDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Update", func() {
		var (
			updateDetails     brokerapi.UpdateDetails
			acceptsIncomplete bool

			dbInstances []*rds.DBInstance
			dbInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error

			modifyDBInstancesInput *rds.ModifyDBInstanceInput
			modifyDBInstanceError  error

			addTagsToResourceInput *rds.AddTagsToResourceInput
			addTagsToResourceError error

			user         *iam.User
			getUserInput *iam.GetUserInput
			getUserError error
		)

		BeforeEach(func() {
			describeDBInstanceError = nil
			modifyDBInstanceError = nil
			addTagsToResourceError = nil
			getUserError = nil

			updateDetails = brokerapi.UpdateDetails{
				ServiceID:  "Service-1",
				PlanID:     "Plan-1",
				Parameters: map[string]interface{}{},
				PreviousValues: brokerapi.PreviousValues{
					PlanID:         "Plan-1",
					ServiceID:      "Service-1",
					OrganizationID: "organization-id",
					SpaceID:        "space-id",
				},
			}
			acceptsIncomplete = true
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()
			iamsvc.Handlers.Clear()

			dbInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				Engine:               aws.String("mysql"),
				EngineVersion:        aws.String("5.6.23"),
			}
			dbInstances = []*rds.DBInstance{dbInstance}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}

			modifyDBInstancesInput = &rds.ModifyDBInstanceInput{}

			addTagsToResourceInput = &rds.AddTagsToResourceInput{
				ResourceName: aws.String("arn:aws:rds:rds-region:account:db:" + dbInstanceIdentifier),
				Tags: []*rds.Tag{
					&rds.Tag{
						Key:   aws.String("Owner"),
						Value: aws.String("Cloud Foundry"),
					},
					&rds.Tag{
						Key:   aws.String("Updated by"),
						Value: aws.String("RDS Service Broker"),
					},
					&rds.Tag{
						Key:   aws.String("Service ID"),
						Value: aws.String("Service-1"),
					},
					&rds.Tag{
						Key:   aws.String("Plan ID"),
						Value: aws.String("Plan-1"),
					},
				},
			}

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ModifyDBInstance|AddTagsToResource"))
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Params).To(Equal(describeDBInstancesInput))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = dbInstances
					r.Error = describeDBInstanceError
				case "ModifyDBInstance":
					// TODO: Expect(r.Params).To(Equal(modifyDBInstancesInput))
					r.Error = modifyDBInstanceError
				case "AddTagsToResource":
					// TODO: Expect(r.Params).To(Equal(addTagsToResourceInput))
					r.Error = addTagsToResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)

			user = &iam.User{
				Arn: aws.String("arn:aws:service:region:account:resource"),
			}
			getUserInput = &iam.GetUserInput{}

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("GetUser"))
				Expect(r.Params).To(Equal(getUserInput))
				data := r.Data.(*iam.GetUserOutput)
				data.User = user
				r.Error = getUserError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("returns the proper response", func() {
			asynch, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
			Expect(asynch).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				updateDetails.Parameters = map[string]interface{}{"backup_retention_period": "invalid"}
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'backup_retention_period' expected type 'int64', got unconvertible type 'string'"))
			})

			Context("and user update parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserUpdateParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				updateDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Plans are not updateable", func() {
			BeforeEach(func() {
				planUpdateable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceNotUpdateable))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				updateDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					describeDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
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
					_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when modifying the DB instance fails", func() {
			BeforeEach(func() {
				modifyDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					modifyDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})

		Context("when adding tags to the DB instance fails", func() {
			BeforeEach(func() {
				addTagsToResourceError = errors.New("operation failed")
			})

			It("does not return an error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when getting the IAM user fails", func() {
			BeforeEach(func() {
				getUserError = errors.New("operation failed")
			})

			It("does not return an error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})

	var _ = Describe("Deprovision", func() {
		var (
			deprovisionDetails brokerapi.DeprovisionDetails
			acceptsIncomplete  bool

			deleteDBInstanceInput *rds.DeleteDBInstanceInput
			deleteDBInstanceError error
		)

		BeforeEach(func() {
			deleteDBInstanceError = nil

			deprovisionDetails = brokerapi.DeprovisionDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}
			acceptsIncomplete = true
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			deleteDBInstanceInput = &rds.DeleteDBInstanceInput{
				DBInstanceIdentifier:      aws.String(dbInstanceIdentifier),
				FinalDBSnapshotIdentifier: nil,
				SkipFinalSnapshot:         aws.Bool(skipFinalSnapshot),
			}

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeleteDBInstance"))
				// TODO: Expect(r.Params).To(Equal(deleteDBInstanceInput))
				r.Error = deleteDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper response", func() {
			asynch, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
			Expect(asynch).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when it does not skip final snaphot", func() {
			BeforeEach(func() {
				skipFinalSnapshot = false
			})

			It("returns the proper response", func() {
				asynch, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(asynch).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				deprovisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when deleting the DB instance fails", func() {
			BeforeEach(func() {
				deleteDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deleteDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
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
					_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Bind", func() {
		var (
			bindDetails brokerapi.BindDetails

			dbInstances []*rds.DBInstance
			dbInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error
		)

		BeforeEach(func() {
			bindDetails = brokerapi.BindDetails{
				ServiceID:  "Service-1",
				PlanID:     "Plan-1",
				AppGUID:    "Application-1",
				Parameters: map[string]interface{}{},
			}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}

			describeDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			dbInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				Engine:               aws.String("test-engine"),
				Endpoint: &rds.Endpoint{
					Address: aws.String("endpoint-address"),
					Port:    aws.Int64(3306),
				},
				DBName:         aws.String("test-db"),
				MasterUsername: aws.String("master-username"),
			}
			dbInstances = []*rds.DBInstance{dbInstance}

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
				Expect(r.Params).To(Equal(describeDBInstancesInput))
				data := r.Data.(*rds.DescribeDBInstancesOutput)
				data.DBInstances = dbInstances
				r.Error = describeDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper response", func() {
			bindingResponse, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
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
			_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
			Expect(dbProvider.GetDatabaseCalled).To(BeTrue())
			Expect(dbProvider.GetDatabaseEngine).To(Equal("test-engine"))
			Expect(database.OpenCalled).To(BeTrue())
			Expect(database.OpenAddress).To(Equal("endpoint-address"))
			Expect(database.OpenPort).To(Equal(int64(3306)))
			Expect(database.OpenName).To(Equal("test-db"))
			Expect(database.OpenUsername).To(Equal("master-username"))
			Expect(database.OpenPassword).ToNot(BeEmpty())
			Expect(database.CreateCalled).To(BeFalse())
			Expect(database.CreateUserCalled).To(BeTrue())
			Expect(database.CreateUserUsername).To(Equal(dbUsername))
			Expect(database.CreateUserPassword).ToNot(BeEmpty())
			Expect(database.GrantPrivilegesCalled).To(BeTrue())
			Expect(database.GrantPrivilegesName).To(Equal("test-db"))
			Expect(database.GrantPrivilegesUsername).To(Equal(dbUsername))
			Expect(database.CloseCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				bindDetails.Parameters = map[string]interface{}{"dbname": true}
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'dbname' expected type 'string', got unconvertible type 'bool'"))
			})

			Context("and user bind parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserBindParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				bindDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Service is not bindable", func() {
			BeforeEach(func() {
				serviceBindable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceNotBindable))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				bindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when getting the database fails", func() {
			BeforeEach(func() {
				dbProvider.GetDatabaseError = errors.New("Database 'unknown' not supported")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Database 'unknown' not supported"))
			})
		})

		Context("when opening a database fails", func() {
			BeforeEach(func() {
				database.OpenError = errors.New("Failed to open database")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open database"))
			})
		})

		Context("when DBNname Parameter is set", func() {
			BeforeEach(func() {
				bindDetails.Parameters = map[string]interface{}{"dbname": "my-test-db"}
			})

			It("returns the proper response", func() {
				bindingResponse, _ := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				credentials := bindingResponse.Credentials.(*CredentialsHash)
				Expect(bindingResponse.SyslogDrainURL).To(BeEmpty())
				Expect(credentials.Name).To(Equal("my-test-db"))
			})

			It("creates the database with the proper name", func() {
				rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(database.CreateCalled).To(BeTrue())
				Expect(database.CreateName).To(Equal("my-test-db"))
				Expect(database.GrantPrivilegesName).To(Equal("my-test-db"))
			})

			Context("when creating the database fails", func() {
				BeforeEach(func() {
					database.CreateError = errors.New("Failed to create database")
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("Failed to create database"))
					Expect(database.CloseCalled).To(BeTrue())
				})
			})
		})

		Context("when creating a user fails", func() {
			BeforeEach(func() {
				database.CreateUserError = errors.New("Failed to create user")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to create user"))
				Expect(database.CloseCalled).To(BeTrue())
			})
		})

		Context("when granting privileges fails", func() {
			BeforeEach(func() {
				database.GrantPrivilegesError = errors.New("Failed to grant privileges")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to grant privileges"))
				Expect(database.CloseCalled).To(BeTrue())
			})
		})
	})

	var _ = Describe("Unbind", func() {
		var (
			unbindDetails brokerapi.UnbindDetails

			dbInstances []*rds.DBInstance
			dbInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error
		)

		BeforeEach(func() {
			unbindDetails = brokerapi.UnbindDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}

			describeDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			dbInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				Engine:               aws.String("test-engine"),
				Endpoint: &rds.Endpoint{
					Address: aws.String("endpoint-address"),
					Port:    aws.Int64(3306),
				},
				DBName:         aws.String("test-db"),
				MasterUsername: aws.String("master-username"),
			}
			dbInstances = []*rds.DBInstance{dbInstance}

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
				Expect(r.Params).To(Equal(describeDBInstancesInput))
				data := r.Data.(*rds.DescribeDBInstancesOutput)
				data.DBInstances = dbInstances
				r.Error = describeDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("makes the proper calls", func() {
			err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
			Expect(dbProvider.GetDatabaseCalled).To(BeTrue())
			Expect(dbProvider.GetDatabaseEngine).To(Equal("test-engine"))
			Expect(database.OpenCalled).To(BeTrue())
			Expect(database.OpenAddress).To(Equal("endpoint-address"))
			Expect(database.OpenPort).To(Equal(int64(3306)))
			Expect(database.OpenName).To(Equal("test-db"))
			Expect(database.OpenUsername).To(Equal("master-username"))
			Expect(database.OpenPassword).ToNot(BeEmpty())
			Expect(database.PrivilegesCalled).To(BeTrue())
			Expect(database.RevokePrivilegesCalled).To(BeFalse())
			Expect(database.DropCalled).To(BeFalse())
			Expect(database.DropUserCalled).To(BeTrue())
			Expect(database.DropUserUsername).To(Equal(dbUsername))
			Expect(database.CloseCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when getting the database fails", func() {
			BeforeEach(func() {
				dbProvider.GetDatabaseError = errors.New("Database 'unknown' not supported")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Database 'unknown' not supported"))
			})
		})

		Context("when opening a database fails", func() {
			BeforeEach(func() {
				database.OpenError = errors.New("Failed to open database")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open database"))
			})
		})

		Context("when getting privileges fails", func() {
			BeforeEach(func() {
				database.PrivilegesError = errors.New("Failed to get privileges")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to get privileges"))
				Expect(database.CloseCalled).To(BeTrue())
			})
		})

		Context("when user has privileges over a db", func() {
			BeforeEach(func() {
				database.PrivilegesPrivileges = map[string][]string{"test-db": []string{dbUsername}}
			})

			It("makes the proper calls", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(database.RevokePrivilegesCalled).To(BeTrue())
				Expect(database.RevokePrivilegesName).To(Equal("test-db"))
				Expect(database.RevokePrivilegesUsername).To(Equal(dbUsername))
				Expect(database.DropCalled).To(BeFalse())
				Expect(database.CloseCalled).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when revoking privileges fails", func() {
				BeforeEach(func() {
					database.RevokePrivilegesError = errors.New("Failed to revoke privileges")
				})

				It("returns the proper error", func() {
					err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("Failed to revoke privileges"))
					Expect(database.CloseCalled).To(BeTrue())
				})
			})

			Context("and the db is not the master db", func() {
				BeforeEach(func() {
					database.PrivilegesPrivileges = map[string][]string{"another-test-db": []string{dbUsername}}
				})

				It("makes the proper calls", func() {
					err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
					Expect(database.RevokePrivilegesCalled).To(BeTrue())
					Expect(database.RevokePrivilegesName).To(Equal("another-test-db"))
					Expect(database.RevokePrivilegesUsername).To(Equal(dbUsername))
					Expect(database.DropCalled).To(BeTrue())
					Expect(database.DropName).To(Equal("another-test-db"))
					Expect(database.CloseCalled).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when droping the db fails", func() {
					BeforeEach(func() {
						database.DropError = errors.New("Failed to drop db")
					})

					It("returns the proper error", func() {
						err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to drop db"))
						Expect(database.CloseCalled).To(BeTrue())
					})
				})

				Context("but there are other users with grants over the db", func() {
					BeforeEach(func() {
						database.PrivilegesPrivileges = map[string][]string{"another-test-db": []string{dbUsername, "another-user"}}
					})

					It("makes the proper calls", func() {
						err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
						Expect(database.RevokePrivilegesCalled).To(BeTrue())
						Expect(database.RevokePrivilegesName).To(Equal("another-test-db"))
						Expect(database.RevokePrivilegesUsername).To(Equal(dbUsername))
						Expect(database.DropCalled).To(BeFalse())
						Expect(database.CloseCalled).To(BeTrue())
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})

		Context("when deleting a user fails", func() {
			BeforeEach(func() {
				database.DropUserError = errors.New("Failed to delete user")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to delete user"))
				Expect(database.CloseCalled).To(BeTrue())
			})
		})
	})

	var _ = Describe("LastOperation", func() {
		var (
			dbInstances                 []*rds.DBInstance
			dbInstance                  *rds.DBInstance
			dbInstanceStatus            string
			pendingModifiedValues       *rds.PendingModifiedValues
			properLastOperationResponse brokerapi.LastOperationResponse
			lastOperationState          string

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error
		)

		BeforeEach(func() {
			pendingModifiedValues = &rds.PendingModifiedValues{}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}

			describeDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			dbInstance = &rds.DBInstance{
				DBInstanceIdentifier:  aws.String(dbInstanceIdentifier),
				DBInstanceStatus:      aws.String(dbInstanceStatus),
				PendingModifiedValues: pendingModifiedValues,
			}
			dbInstances = []*rds.DBInstance{dbInstance}

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
				Expect(r.Params).To(Equal(describeDBInstancesInput))
				data := r.Data.(*rds.DescribeDBInstancesOutput)
				data.DBInstances = dbInstances
				r.Error = describeDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)

			properLastOperationResponse = brokerapi.LastOperationResponse{
				State:       lastOperationState,
				Description: "DB Instance '" + dbInstanceIdentifier + "' status is '" + dbInstanceStatus + "'",
			}
		})

		Context("when instance is not found", func() {
			JustBeforeEach(func() {
				dbInstances = []*rds.DBInstance{}
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.LastOperation(instanceID)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.LastOperation(instanceID)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					describeDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.LastOperation(instanceID)
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
					_, err := rdsBroker.LastOperation(instanceID)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when last operation is still in progress", func() {
			BeforeEach(func() {
				dbInstanceStatus = "creating"
				lastOperationState = brokerapi.LastOperationInProgress
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "failed"
				lastOperationState = brokerapi.LastOperationFailed
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation succeeded", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				lastOperationState = brokerapi.LastOperationSucceeded
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("but has pending modifications", func() {
				BeforeEach(func() {
					pendingModifiedValues = &rds.PendingModifiedValues{
						AllocatedStorage: aws.Int64(100),
					}
				})

				JustBeforeEach(func() {
					properLastOperationResponse = brokerapi.LastOperationResponse{
						State:       brokerapi.LastOperationInProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending modifications",
					}
				})

				It("returns the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})
		})
	})
})
