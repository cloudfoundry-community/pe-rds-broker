package rdsbroker_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cf-platform-eng/rds-broker/rdsbroker"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/frodenas/brokerapi"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("RDS Broker", func() {
	var (
		rdsBroker *RDSBroker

		logger   = lager.NewLogger("rdsbroker_test")
		testSink = lagertest.NewTestSink()

		iamsvc = iam.New(nil)
		rdssvc = rds.New(nil)

		rdsProperties = RDSProperties{
			DBInstanceClass:  "db.m3.medium",
			Engine:           "mysql",
			EngineVersion:    "5.6.23",
			AllocatedStorage: 100,
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
			ID:          "Service-1",
			Name:        "Service 1",
			Description: "This is the Service 1",
			Plans:       []ServicePlan{plan1},
		}
		service2 = Service{
			ID:          "Service-2",
			Name:        "Service 2",
			Description: "This is the Service 2",
			Plans:       []ServicePlan{plan2},
		}

		catalog = Catalog{
			Services: []Service{service1, service2},
		}

		config = Config{
			Region:   "rds-region",
			DBPrefix: "cf",
			Catalog:  catalog,
		}

		instanceID           = "instance-id"
		dbInstanceIdentifier = "cf-instance-id"
	)

	BeforeEach(func() {
		logger.RegisterSink(testSink)
		rdsBroker = New(config, logger, iamsvc, rdssvc)
	})

	var _ = Describe("Services", func() {
		var (
			properCatalogResponse brokerapi.CatalogResponse
		)

		BeforeEach(func() {
			properCatalogResponse = brokerapi.CatalogResponse{
				Services: []brokerapi.Service{
					brokerapi.Service{
						ID:          "Service-1",
						Name:        "Service 1",
						Description: "This is the Service 1",
						Plans: []brokerapi.ServicePlan{
							brokerapi.ServicePlan{
								ID:          "Plan-1",
								Name:        "Plan 1",
								Description: "This is the Plan 1",
							},
						},
					},
					brokerapi.Service{
						ID:          "Service-2",
						Name:        "Service 2",
						Description: "This is the Service 2",
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

	var _ = Describe("LastOperation", func() {
		var (
			dbInstances                 []*rds.DBInstance
			dbInstance                  *rds.DBInstance
			dbInstanceStatus            string
			pendingModifiedValues       *rds.PendingModifiedValues
			properLastOperationResponse brokerapi.LastOperationResponse
			lastOperationState          string

			describeDBInstancesCall func(r *request.Request)
		)

		BeforeEach(func() {
			rdssvc.Handlers.Clear()
			pendingModifiedValues = &rds.PendingModifiedValues{}
		})

		JustBeforeEach(func() {
			dbInstance = &rds.DBInstance{
				DBInstanceIdentifier:  aws.String(dbInstanceIdentifier),
				DBInstanceStatus:      aws.String(dbInstanceStatus),
				PendingModifiedValues: pendingModifiedValues,
			}
			dbInstances = []*rds.DBInstance{dbInstance}

			describeDBInstancesCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
				data := r.Data.(*rds.DescribeDBInstancesOutput)
				data.DBInstances = dbInstances
			}
			rdssvc.Handlers.Send.PushBack(describeDBInstancesCall)

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
