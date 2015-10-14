package fakes

import "github.com/frodenas/brokerapi"

type FakeServiceBroker struct {
	BrokerCalled bool

	ProvisionInstanceID        string
	ProvisionDetails           brokerapi.ProvisionDetails
	ProvisionAcceptsIncomplete bool
	ProvisionResponse          brokerapi.ProvisioningResponse
	ProvisionAsynch            bool
	ProvisionError             error

	UpdateInstanceID        string
	UpdateDetails           brokerapi.UpdateDetails
	UpdateAcceptsIncomplete bool
	UpdateAsynch            bool
	UpdateError             error

	DeprovisionInstanceID        string
	DeprovisionDetails           brokerapi.DeprovisionDetails
	DeprovisionAcceptsIncomplete bool
	DeprovisionAsynch            bool
	DeprovisionError             error

	BindInstanceID string
	BindBindingID  string
	BindDetails    brokerapi.BindDetails
	BindResponse   brokerapi.BindingResponse
	BindError      error

	UnbindInstanceID string
	UnbindBindingID  string
	UnbindDetails    brokerapi.UnbindDetails
	UnbindError      error

	LastOperationInstanceID string
	LastOperationResponse   brokerapi.LastOperationResponse
	LastOperationError      error
}

func (fakeBroker *FakeServiceBroker) Services() brokerapi.CatalogResponse {
	fakeBroker.BrokerCalled = true

	return brokerapi.CatalogResponse{
		[]brokerapi.Service{
			brokerapi.Service{
				ID:          "0A789746-596F-4CEA-BFAC-A0795DA056E3",
				Name:        "p-cassandra",
				Description: "Cassandra service for application development and testing",
				Bindable:    true,
				Tags: []string{
					"pivotal",
					"cassandra",
				},
				Metadata: &brokerapi.ServiceMetadata{
					DisplayName:         "Cassandra",
					ImageURL:            "http://foo.com/thing.png",
					LongDescription:     "Long description",
					ProviderDisplayName: "Pivotal",
					DocumentationURL:    "http://thedocs.com",
					SupportURL:          "http://helpme.no",
				},
				PlanUpdateable: true,
				Plans: []brokerapi.ServicePlan{
					brokerapi.ServicePlan{
						ID:          "ABE176EE-F69F-4A96-80CE-142595CC24E3",
						Name:        "default",
						Description: "The default Cassandra plan",
						Metadata: &brokerapi.ServicePlanMetadata{
							Bullets: []string{"bullet-1"},
							Costs: []brokerapi.Cost{
								brokerapi.Cost{
									Amount: map[string]interface{}{"usd": 99, "eur": 49},
									Unit:   "MONTHLY",
								},
							},
							DisplayName: "Cassandra",
						},
						Free: false,
					},
				},
				DashboardClient: &brokerapi.DashboardClient{
					ID:          "dashboard-id",
					Secret:      "dashboard-secret",
					RedirectURI: "http://dashboard-redirect",
				},
			},
		},
	}
}

func (fakeBroker *FakeServiceBroker) Provision(instanceID string, details brokerapi.ProvisionDetails, acceptsIncomplete bool) (brokerapi.ProvisioningResponse, bool, error) {
	fakeBroker.BrokerCalled = true
	fakeBroker.ProvisionInstanceID = instanceID
	fakeBroker.ProvisionDetails = details
	fakeBroker.ProvisionAcceptsIncomplete = acceptsIncomplete

	return fakeBroker.ProvisionResponse, fakeBroker.ProvisionAsynch, fakeBroker.ProvisionError
}

func (fakeBroker *FakeServiceBroker) Update(instanceID string, details brokerapi.UpdateDetails, acceptsIncomplete bool) (bool, error) {
	fakeBroker.BrokerCalled = true
	fakeBroker.UpdateInstanceID = instanceID
	fakeBroker.UpdateDetails = details
	fakeBroker.UpdateAcceptsIncomplete = acceptsIncomplete

	return fakeBroker.UpdateAsynch, fakeBroker.UpdateError
}

func (fakeBroker *FakeServiceBroker) Deprovision(instanceID string, details brokerapi.DeprovisionDetails, acceptsIncomplete bool) (bool, error) {
	fakeBroker.BrokerCalled = true
	fakeBroker.DeprovisionInstanceID = instanceID
	fakeBroker.DeprovisionDetails = details
	fakeBroker.DeprovisionAcceptsIncomplete = acceptsIncomplete

	return fakeBroker.DeprovisionAsynch, fakeBroker.DeprovisionError
}

func (fakeBroker *FakeServiceBroker) Bind(instanceID string, bindingID string, details brokerapi.BindDetails) (brokerapi.BindingResponse, error) {
	fakeBroker.BrokerCalled = true
	fakeBroker.BindInstanceID = instanceID
	fakeBroker.BindBindingID = bindingID
	fakeBroker.BindDetails = details

	return fakeBroker.BindResponse, fakeBroker.BindError
}

func (fakeBroker *FakeServiceBroker) Unbind(instanceID string, bindingID string, details brokerapi.UnbindDetails) error {
	fakeBroker.BrokerCalled = true
	fakeBroker.UnbindInstanceID = instanceID
	fakeBroker.UnbindBindingID = bindingID
	fakeBroker.UnbindDetails = details

	return fakeBroker.UnbindError
}

func (fakeBroker *FakeServiceBroker) LastOperation(instanceID string) (brokerapi.LastOperationResponse, error) {
	fakeBroker.BrokerCalled = true
	fakeBroker.LastOperationInstanceID = instanceID

	return fakeBroker.LastOperationResponse, fakeBroker.LastOperationError
}
