package brokerapi_test

import (
	. "github.com/frodenas/brokerapi"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/frodenas/brokerapi/matchers"
)

var _ = Describe("Catalog", func() {
	var (
		plan1 = ServicePlan{ID: "Plan-1"}
		plan2 = ServicePlan{ID: "Plan-2"}

		service1 = Service{ID: "Service-1", Plans: []ServicePlan{plan1}}
		service2 = Service{ID: "Service-2", Plans: []ServicePlan{plan2}}

		catalog Catalog
	)

	Describe("JSON encoding", func() {
		BeforeEach(func() {
			catalog = Catalog{}
		})

		It("uses the correct keys", func() {
			json := `{"services":null}`

			Expect(catalog).To(matchers.MarshalToJSON(json))
		})
	})

	Describe("Validate", func() {
		BeforeEach(func() {
			catalog = Catalog{}
		})

		It("does not return error if all fields are valid", func() {
			err := catalog.Validate()

			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if Services are not valid", func() {
			catalog.Services = []Service{
				Service{},
			}

			err := catalog.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Validating Services configuration"))
		})
	})

	Describe("FindService", func() {
		BeforeEach(func() {
			catalog = Catalog{
				Services: []Service{service1, service2},
			}
		})

		It("returns the Service if it is found", func() {
			service, found := catalog.FindService("Service-1")
			Expect(service).To(Equal(service1))
			Expect(found).To(BeTrue())
		})

		It("returns false if it is not found", func() {
			_, found := catalog.FindService("Service-?")
			Expect(found).To(BeFalse())
		})
	})

	Describe("FindServicePlan", func() {
		BeforeEach(func() {
			catalog = Catalog{
				Services: []Service{service1, service2},
			}
		})

		It("returns the Service Plan if it is found", func() {
			plan, found := catalog.FindServicePlan("Plan-1")
			Expect(plan).To(Equal(plan1))
			Expect(found).To(BeTrue())
		})

		It("returns false if it is not found", func() {
			_, found := catalog.FindServicePlan("Plan-?")
			Expect(found).To(BeFalse())
		})
	})
})

var _ = Describe("Service", func() {
	var (
		service Service

		validService = Service{
			ID:              "ID-1",
			Name:            "Cassandra",
			Description:     "A Cassandra Plan",
			Bindable:        true,
			Tags:            []string{"cassandra"},
			Metadata:        &ServiceMetadata{},
			Requires:        []string{"syslog"},
			PlanUpdateable:  true,
			Plans:           []ServicePlan{},
			DashboardClient: &DashboardClient{},
		}
	)

	BeforeEach(func() {
		service = validService
	})

	Describe("JSON encoding", func() {
		It("uses the correct keys", func() {
			json := `{"id":"ID-1","name":"Cassandra","description":"A Cassandra Plan","bindable":true,"tags":["cassandra"],"metadata":{},"requires":["syslog"],"plan_updateable":true,"plans":[],"dashboard_client":{}}`

			Expect(service).To(matchers.MarshalToJSON(json))
		})
	})

	Describe("Validate", func() {
		It("does not return error if all fields are valid", func() {
			err := service.Validate()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if ID is empty", func() {
			service.ID = ""

			err := service.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty ID"))
		})

		It("returns error if Name is empty", func() {
			service.Name = ""

			err := service.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Name"))
		})

		It("returns error if Description is empty", func() {
			service.Description = ""

			err := service.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Description"))
		})

		It("returns error if Plans are not valid", func() {
			service.Plans = []ServicePlan{
				ServicePlan{},
			}

			err := service.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Validating Plans configuration"))
		})
	})
})

var _ = Describe("ServiceMetadata", func() {
	var (
		serviceMetadata ServiceMetadata

		validServiceMetadata = ServiceMetadata{
			DisplayName:         "Cassandra",
			ImageURL:            "http://foo.com/thing.png",
			LongDescription:     "A long description of Cassandra",
			ProviderDisplayName: "Pivotal",
			DocumentationURL:    "http://thedocs.com",
			SupportURL:          "http://helpme.no",
		}
	)

	BeforeEach(func() {
		serviceMetadata = validServiceMetadata
	})

	Describe("JSON encoding", func() {
		It("uses the correct keys", func() {
			json := `{"displayName":"Cassandra","imageUrl":"http://foo.com/thing.png","longDescription":"A long description of Cassandra","providerDisplayName":"Pivotal","documentationUrl":"http://thedocs.com","supportUrl":"http://helpme.no"}`

			Expect(serviceMetadata).To(matchers.MarshalToJSON(json))
		})
	})
})

var _ = Describe("ServicePlan", func() {
	var (
		servicePlan ServicePlan

		validServicePlan = ServicePlan{
			ID:          "ID-1",
			Name:        "Cassandra",
			Description: "A Cassandra Plan",
			Metadata:    &ServicePlanMetadata{},
			Free:        true,
		}
	)

	BeforeEach(func() {
		servicePlan = validServicePlan
	})

	Describe("JSON encoding", func() {
		It("uses the correct keys", func() {
			json := `{"id":"ID-1","name":"Cassandra","description":"A Cassandra Plan","metadata":{},"free":true}`

			Expect(servicePlan).To(matchers.MarshalToJSON(json))
		})
	})

	Describe("Validate", func() {
		It("does not return error if all fields are valid", func() {
			err := servicePlan.Validate()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if ID is empty", func() {
			servicePlan.ID = ""

			err := servicePlan.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty ID"))
		})

		It("returns error if Name is empty", func() {
			servicePlan.Name = ""

			err := servicePlan.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Name"))
		})

		It("returns error if Description is empty", func() {
			servicePlan.Description = ""

			err := servicePlan.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Description"))
		})
	})
})

var _ = Describe("ServicePlanMetadata", func() {
	var (
		servicePlanMetadata ServicePlanMetadata

		validServicePlanMetadata = ServicePlanMetadata{
			Bullets:     []string{},
			Costs:       []Cost{},
			DisplayName: "Some display name",
		}
	)

	BeforeEach(func() {
		servicePlanMetadata = validServicePlanMetadata
	})

	Describe("JSON encoding", func() {
		It("uses the correct keys", func() {
			json := `{"displayName":"Some display name"}`

			Expect(servicePlanMetadata).To(matchers.MarshalToJSON(json))
		})
	})
})

var _ = Describe("DashboardClient", func() {
	var (
		dashboardClient DashboardClient

		validDashboardClient = DashboardClient{
			ID:          "ID-1",
			Secret:      "dashboard-secret",
			RedirectURI: "redirect-uri",
		}
	)

	BeforeEach(func() {
		dashboardClient = validDashboardClient
	})

	Describe("JSON encoding", func() {
		It("uses the correct keys", func() {
			json := `{"id":"ID-1","secret":"dashboard-secret","redirect_uri":"redirect-uri"}`

			Expect(dashboardClient).To(matchers.MarshalToJSON(json))
		})
	})
})

var _ = Describe("Cost", func() {
	var (
		cost Cost

		validCost = Cost{
			Amount: map[string]interface{}{"usd": 99, "eur": 49},
			Unit:   "MONTHLY",
		}
	)

	BeforeEach(func() {
		cost = validCost
	})

	Describe("JSON encoding", func() {
		It("uses the correct keys", func() {
			json := `{"amount":{"eur":49,"usd":99},"unit":"MONTHLY"}`

			Expect(cost).To(matchers.MarshalToJSON(json))
		})
	})
})
