package brokerapi_test

import (
	. "github.com/frodenas/brokerapi"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/frodenas/brokerapi/matchers"
)

var _ = Describe("Error Response", func() {
	Describe("JSON encoding", func() {
		It("has a description field", func() {
			errorResponse := ErrorResponse{}
			json := `{"description":""}`

			Expect(errorResponse).To(matchers.MarshalToJSON(json))
		})

		Context("when an error is present", func() {
			It("has an error field", func() {
				errorResponse := ErrorResponse{
					Error:       "an error happened",
					Description: "a bad thing happened",
				}
				json := `{"error":"an error happened","description":"a bad thing happened"}`

				Expect(errorResponse).To(matchers.MarshalToJSON(json))
			})
		})
	})
})

var _ = Describe("Catalog Response", func() {
	Describe("JSON encoding", func() {
		It("has a list of services", func() {
			catalogResponse := CatalogResponse{
				Services: []Service{},
			}
			json := `{"services":[]}`

			Expect(catalogResponse).To(matchers.MarshalToJSON(json))
		})
	})
})

var _ = Describe("Provisioning Response", func() {
	Describe("JSON encoding", func() {
		Context("when the dashboard URL is not present", func() {
			It("does not return it in the JSON", func() {
				provisioningResponse := ProvisioningResponse{}
				json := `{}`

				Expect(provisioningResponse).To(matchers.MarshalToJSON(json))
			})
		})

		Context("when the dashboard URL is present", func() {
			It("returns it in the JSON", func() {
				provisioningResponse := ProvisioningResponse{
					DashboardURL: "http://example.com/broker",
				}
				json := `{"dashboard_url":"http://example.com/broker"}`

				Expect(provisioningResponse).To(matchers.MarshalToJSON(json))
			})
		})
	})
})

var _ = Describe("Binding Response", func() {
	Describe("JSON encoding", func() {
		It("has a credentials field", func() {
			bindingResponse := BindingResponse{}
			json := `{"credentials":null}`

			Expect(bindingResponse).To(matchers.MarshalToJSON(json))
		})

		Context("when a Syslog Drain URL is present", func() {
			It("has a syslog_drain_url field", func() {
				bindingResponse := BindingResponse{
					SyslogDrainURL: "http://example.com/syslogdrain",
				}
				json := `{"credentials":null,"syslog_drain_url":"http://example.com/syslogdrain"}`

				Expect(bindingResponse).To(matchers.MarshalToJSON(json))
			})
		})
	})
})

var _ = Describe("Last Operation Response", func() {
	Describe("JSON encoding", func() {
		It("has a state field", func() {
			lastOperationResponse := LastOperationResponse{}
			json := `{"state":""}`

			Expect(lastOperationResponse).To(matchers.MarshalToJSON(json))
		})

		It("operation in progress", func() {
			lastOperationResponse := LastOperationResponse{
				State: LastOperationInProgress,
			}
			json := `{"state":"in progress"}`

			Expect(lastOperationResponse).To(matchers.MarshalToJSON(json))
		})

		It("operation failed", func() {
			lastOperationResponse := LastOperationResponse{
				State: LastOperationFailed,
			}
			json := `{"state":"failed"}`

			Expect(lastOperationResponse).To(matchers.MarshalToJSON(json))
		})

		It("operation succeeded", func() {
			lastOperationResponse := LastOperationResponse{
				State: LastOperationSucceeded,
			}
			json := `{"state":"succeeded"}`

			Expect(lastOperationResponse).To(matchers.MarshalToJSON(json))
		})

		Context("when a description is present", func() {
			It("has a description field", func() {
				lastOperationResponse := LastOperationResponse{
					State:       LastOperationSucceeded,
					Description: "the operation succeeded",
				}
				json := `{"state":"succeeded","description":"the operation succeeded"}`

				Expect(lastOperationResponse).To(matchers.MarshalToJSON(json))
			})
		})
	})
})
