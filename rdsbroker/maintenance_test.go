package rdsbroker_test

import (
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/cloudfoundry-community/pe-rds-broker/awsrds"
	rdsfake "github.com/cloudfoundry-community/pe-rds-broker/awsrds/fakes"
	. "github.com/cloudfoundry-community/pe-rds-broker/rdsbroker"
	sqlfake "github.com/cloudfoundry-community/pe-rds-broker/sqlengine/fakes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Maintenance", func() {
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

		sqlEngine   *sqlfake.FakeSQLEngine
		sqlProvider *sqlfake.FakeProvider

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsBroker *RDSBroker

		allowUserProvisionParameters bool
		allowUserUpdateParameters    bool
		allowUserBindParameters      bool
		serviceBindable              bool
		planUpdateable               bool
		skipFinalSnapshot            bool
		serviceBrokerID              string
		passwordSalt                 string
	)

	BeforeEach(func() {
		allowUserProvisionParameters = true
		allowUserUpdateParameters = true
		allowUserBindParameters = true
		serviceBindable = true
		planUpdateable = true
		skipFinalSnapshot = true
		serviceBrokerID = ""
		passwordSalt = "SuperSecureSaltThing"

		dbInstance = &rdsfake.FakeDBInstance{}
		dbCluster = &rdsfake.FakeDBCluster{}

		dbCluster.ModifyCount = 0
		dbInstance.ModifyCount = 0

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

		dbCluster.ListDBClustersDetails = []awsrds.DBClusterDetails{
			{
				Identifier:     "cf-cluster-1",
				Endpoint:       "endpoint-address",
				Port:           3306,
				DatabaseName:   "test-db",
				MasterUsername: "master-username",
				Tags:           map[string]string{"Owner": "Cloud Foundry", "Foo": "BAR"},
			},
			{
				Identifier:     "cf-cluster-2",
				Endpoint:       "endpoint-address",
				Port:           3306,
				DatabaseName:   "test-db",
				MasterUsername: "master-username",
				Tags:           map[string]string{"Owner": "Cloud Foundry", "Foo": "BAR"},
			},
		}
		dbInstance.ListDBInstancesDetails = []awsrds.DBInstanceDetails{
			{
				Identifier:     "cf-instance-1",
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
				MultiAZ:        true,
				Tags:           map[string]string{"Owner": "Cloud Foundry", "Foo": "BAR"},
			},
			{
				Identifier:     "cf-instance-2",
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
				MultiAZ:        true,
				Tags:           map[string]string{"Owner": "Cloud Foundry", "Foo": "BAR"},
			},
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
			MasterPasswordSalt:           passwordSalt,
		}

		logger = lager.NewLogger("rdsbroker_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)
		rdsBroker = New(config, dbInstance, dbCluster, sqlProvider, logger)
	})
	var _ = Describe("UpdatePasswords", func() {

		It("Is updating cluster passwords", func() {
			err := UpdatePasswords(rdsBroker)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbCluster.ModifyCount).Should(Equal(2))
			Expect(dbCluster.ModifyApplyImmediately).Should(BeTrue())
			Expect(dbCluster.ModifyCalled).Should(BeTrue())
			Expect(dbCluster.ModifyDBClusterDetails.MasterUserPassword).Should(Equal("Y1NsdXVwc2V0cmVTcmUtYzJ11B2M2Y8A"))
			Expect(dbCluster.ModifyDBClusterDetails.DatabaseName).Should(Equal("test-db"))
		})

		It("Is updating instance passwords", func() {
			err := UpdatePasswords(rdsBroker)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstance.ModifyCount).Should(Equal(2))
			Expect(dbInstance.ModifyApplyImmediately).Should(BeTrue())
			Expect(dbInstance.ModifyCalled).Should(BeTrue())
			Expect(dbInstance.ModifyDBInstanceDetails.MasterUserPassword).Should(Equal("aVNudXNwdGVhcm5TY2VlYy11MnLUHYzZ"))
			Expect(dbInstance.ModifyDBInstanceDetails.DBName).Should(Equal("test-db"))
			Expect(dbInstance.ModifyDBInstanceDetails.MultiAZ).Should(BeTrue())
		})
	})

})
