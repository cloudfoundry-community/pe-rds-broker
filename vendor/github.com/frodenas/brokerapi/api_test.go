package brokerapi_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/frodenas/brokerapi"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/frodenas/brokerapi/fakes"

	"github.com/drewolson/testflight"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("Service Broker API", func() {
	var brokerAPI http.Handler
	var fakeServiceBroker *fakes.FakeServiceBroker
	var brokerLogger *lagertest.TestLogger
	var credentials = BrokerCredentials{
		Username: "username",
		Password: "password",
	}

	lastLogLine := func() lager.LogFormat {
		if len(brokerLogger.Logs()) == 0 {
			// better way to raise error?
			err := errors.New("expected some log lines but there were none!")
			Expect(err).NotTo(HaveOccurred())
		}

		return brokerLogger.Logs()[0]
	}

	BeforeEach(func() {
		fakeServiceBroker = &fakes.FakeServiceBroker{}
		brokerLogger = lagertest.NewTestLogger("broker-api")
		brokerAPI = New(fakeServiceBroker, brokerLogger, credentials)
	})

	Describe("respose headers", func() {
		makeRequest := func() *httptest.ResponseRecorder {
			recorder := httptest.NewRecorder()
			request, _ := http.NewRequest("GET", "/v2/catalog", nil)
			request.SetBasicAuth(credentials.Username, credentials.Password)
			brokerAPI.ServeHTTP(recorder, request)
			return recorder
		}

		It("has a Content-Type header", func() {
			response := makeRequest()

			header := response.Header().Get("Content-Type")
			Ω(header).Should(Equal("application/json"))
		})
	})

	Describe("authentication", func() {
		makeRequestWithoutAuth := func() *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				request, _ := http.NewRequest("GET", "/v2/catalog", nil)
				response = r.Do(request)
			})
			return response
		}

		makeRequestWithAuth := func(username string, password string) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				request, _ := http.NewRequest("GET", "/v2/catalog", nil)
				request.SetBasicAuth(username, password)

				response = r.Do(request)
			})
			return response
		}

		makeRequestWithUnrecognizedAuth := func() *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				request, _ := http.NewRequest("GET", "/v2/catalog", nil)
				// dXNlcm5hbWU6cGFzc3dvcmQ= is base64 encoding of 'username:password',
				// ie, a correctly encoded basic authorization header
				request.Header["Authorization"] = []string{"NOTBASIC dXNlcm5hbWU6cGFzc3dvcmQ="}

				response = r.Do(request)
			})
			return response
		}

		It("returns 401 when the authorization header has an incorrect password", func() {
			response := makeRequestWithAuth("username", "fake_password")
			Expect(response.StatusCode).To(Equal(401))
		})

		It("returns 401 when the authorization header has an incorrect username", func() {
			response := makeRequestWithAuth("fake_username", "password")
			Expect(response.StatusCode).To(Equal(401))
		})

		It("returns 401 when there is no authorization header", func() {
			response := makeRequestWithoutAuth()
			Expect(response.StatusCode).To(Equal(401))
		})

		It("returns 401 when there is a unrecognized authorization header", func() {
			response := makeRequestWithUnrecognizedAuth()
			Expect(response.StatusCode).To(Equal(401))
		})

		It("does not call through to the service broker when not authenticated", func() {
			makeRequestWithAuth("username", "fake_password")
			Ω(fakeServiceBroker.BrokerCalled).ShouldNot(BeTrue(),
				"broker should not have been hit when authentication failed",
			)
		})
	})

	Describe("services", func() {
		makeServicesRequest := func() *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				request, _ := http.NewRequest("GET", "/v2/catalog", nil)
				request.SetBasicAuth("username", "password")

				response = r.Do(request)
			})
			return response
		}

		It("returns a 200", func() {
			response := makeServicesRequest()
			Expect(response.StatusCode).To(Equal(200))
		})

		It("returns valid catalog json", func() {
			response := makeServicesRequest()
			Expect(response.Body).To(MatchJSON(fixture("catalog.json")))
		})
	})

	Describe("provision", func() {
		var provisionInstanceID string
		var provisionDetails ProvisionDetails
		var provisionAcceptsIncomplete bool

		makeProvisionRequest := func(instanceID string, provisionDetails ProvisionDetails, acceptsIncomplete bool) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				path := fmt.Sprintf("/v2/service_instances/%s", instanceID)
				if acceptsIncomplete {
					path = path + "?accepts_incomplete=true"
				}

				buffer := &bytes.Buffer{}
				json.NewEncoder(buffer).Encode(provisionDetails)
				request, err := http.NewRequest("PUT", path, buffer)
				Expect(err).NotTo(HaveOccurred())
				request.Header.Add("Content-Type", "application/json")
				request.SetBasicAuth(credentials.Username, credentials.Password)

				response = r.Do(request)
			})
			return response
		}

		BeforeEach(func() {
			provisionInstanceID = uniqueInstanceID()
			provisionDetails = ProvisionDetails{
				OrganizationGUID: "organization-guid",
				PlanID:           "plan-id",
				ServiceID:        "service-id",
				SpaceGUID:        "space-guid",
			}
			provisionAcceptsIncomplete = true

			fakeServiceBroker.ProvisionResponse = ProvisioningResponse{}
			fakeServiceBroker.ProvisionAsynch = false
			fakeServiceBroker.ProvisionError = nil
		})

		It("calls Provision on the service broker with the instance id", func() {
			makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
			Expect(fakeServiceBroker.ProvisionInstanceID).To(Equal(provisionInstanceID))
		})

		It("calls Provision on the service broker with the provision details", func() {
			makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
			Expect(fakeServiceBroker.ProvisionDetails).To(Equal(provisionDetails))
		})

		It("calls Provision on the service broker with accepts imcomplete", func() {
			makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
			Expect(fakeServiceBroker.ProvisionAcceptsIncomplete).To(Equal(provisionAcceptsIncomplete))
		})

		It("returns a 201", func() {
			response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
			Expect(response.StatusCode).To(Equal(201))
		})

		It("returns proper json", func() {
			response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
			Expect(response.Body).To(MatchJSON(fixture("provision.json")))
		})

		Context("when broker returns a dashboard_url field", func() {
			BeforeEach(func() {
				fakeServiceBroker.ProvisionResponse = ProvisioningResponse{DashboardURL: "dashboard-url"}
			})

			It("returns proper json", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(fixture("provision_dashboard.json")))
			})
		})

		Context("when broker is asynchronous", func() {
			BeforeEach(func() {
				fakeServiceBroker.ProvisionAsynch = true
			})

			It("returns a 202", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(202))
			})

			It("returns proper json", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(fixture("provision.json")))
			})
		})

		Context("when the instance already exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.ProvisionError = ErrInstanceAlreadyExists
			})

			It("returns a 409", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(409))
			})

			It("returns an empty JSON object", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{}`))
			})

			It("logs an appropriate error", func() {
				makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("provision.instance-already-exists"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance already exists"))
			})
		})

		Context("when the instance limit has been reached", func() {
			BeforeEach(func() {
				fakeServiceBroker.ProvisionError = ErrInstanceLimitMet
			})

			It("returns a 500", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(fixture("instance_limit_error.json")))
			})

			It("logs an appropriate error", func() {
				makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)

				Expect(lastLogLine().Message).To(ContainSubstring("provision.instance-limit-reached"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance limit for this service has been reached"))
			})
		})

		Context("when asyncronous operation is required", func() {
			BeforeEach(func() {
				fakeServiceBroker.ProvisionError = ErrAsyncRequired
			})

			It("returns a 422", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(422))
			})

			It("returns proper json", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(fixture("instance_async_required.json")))
			})

			It("logs an appropriate error", func() {
				makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("provision.instance-async-required"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("This service plan requires client support for asynchronous service operations."))
			})
		})

		Context("when an unexpected error occurs", func() {
			BeforeEach(func() {
				fakeServiceBroker.ProvisionError = errors.New("broker failed")
			})

			It("returns a 500", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{"description":"broker failed"}`))
			})

			It("logs an appropriate error", func() {
				makeProvisionRequest(provisionInstanceID, provisionDetails, provisionAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("provision.unknown-error"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("broker failed"))
			})
		})

		Context("when we send invalid json", func() {
			makeBadProvisionRequest := func(instanceID string) *testflight.Response {
				response := &testflight.Response{}

				testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
					path := fmt.Sprintf("/v2/service_instances/%s", instanceID)

					body := strings.NewReader("{{{{{")
					request, err := http.NewRequest("PUT", path, body)
					Expect(err).NotTo(HaveOccurred())
					request.Header.Add("Content-Type", "application/json")
					request.SetBasicAuth(credentials.Username, credentials.Password)

					response = r.Do(request)
				})

				return response
			}

			It("returns a 400 bad request", func() {
				response := makeBadProvisionRequest(provisionInstanceID)
				Expect(response.StatusCode).Should(Equal(400))
			})

			It("logs a message", func() {
				makeBadProvisionRequest(provisionInstanceID)
				Expect(lastLogLine().Message).To(ContainSubstring("provision.invalid-provision-details"))
			})
		})
	})

	Describe("update", func() {
		var updateInstanceID string
		var updateDetails UpdateDetails
		var updateAcceptsIncomplete bool

		makeUpdateRequest := func(instanceID string, updateDetails UpdateDetails, acceptsIncomplete bool) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				path := fmt.Sprintf("/v2/service_instances/%s", instanceID)
				if acceptsIncomplete {
					path = path + "?accepts_incomplete=true"
				}

				buffer := &bytes.Buffer{}
				json.NewEncoder(buffer).Encode(updateDetails)
				request, err := http.NewRequest("PATCH", path, buffer)
				Expect(err).NotTo(HaveOccurred())
				request.Header.Add("Content-Type", "application/json")
				request.SetBasicAuth(credentials.Username, credentials.Password)

				response = r.Do(request)
			})
			return response
		}

		BeforeEach(func() {
			updateInstanceID = uniqueInstanceID()
			updateDetails = UpdateDetails{
				ServiceID: "service-id",
				PlanID:    "plan-id",
				PreviousValues: PreviousValues{
					PlanID:         "previous-plan-id",
					ServiceID:      "previous-service-id",
					OrganizationID: "previous-organization-id",
					SpaceID:        "previous-space-id",
				},
			}
			updateAcceptsIncomplete = true

			fakeServiceBroker.UpdateAsynch = false
			fakeServiceBroker.UpdateError = nil
		})

		It("calls Update on the service broker with the instance id", func() {
			makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
			Expect(fakeServiceBroker.UpdateInstanceID).To(Equal(updateInstanceID))
		})

		It("calls Update on the service broker with the update details", func() {
			makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
			Expect(fakeServiceBroker.UpdateDetails).To(Equal(updateDetails))
		})

		It("calls Update on the service broker with accepts imcomplete", func() {
			makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
			Expect(fakeServiceBroker.UpdateAcceptsIncomplete).To(Equal(updateAcceptsIncomplete))
		})

		It("returns a 200", func() {
			response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
			Expect(response.StatusCode).To(Equal(200))
		})

		It("returns an empty JSON object", func() {
			response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
			Expect(response.Body).To(MatchJSON(`{}`))
		})

		Context("when broker is asynchronous", func() {
			BeforeEach(func() {
				fakeServiceBroker.UpdateAsynch = true
			})

			It("returns a 202", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(202))
			})

			It("returns an empty JSON object", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{}`))
			})
		})

		Context("when the instance does not exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.UpdateError = ErrInstanceDoesNotExist
			})

			It("returns a 500", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns an empty JSON object", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{}`))
			})

			It("logs an appropriate error", func() {
				makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("update.instance-missing"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance does not exist"))
			})
		})

		Context("when asyncronous operation is required", func() {
			BeforeEach(func() {
				fakeServiceBroker.UpdateError = ErrAsyncRequired
			})

			It("returns a 422", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(422))
			})

			It("eturns proper json", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(fixture("instance_async_required.json")))
			})

			It("logs an appropriate error", func() {
				makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("update.instance-async-required"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("This service plan requires client support for asynchronous service operations."))
			})
		})

		Context("when instance is not updateable", func() {
			BeforeEach(func() {
				fakeServiceBroker.UpdateError = ErrInstanceNotUpdateable
			})

			It("returns a 500", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{"description":"instance is not updateable"}`))
			})

			It("logs an appropriate error", func() {
				makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("update.instance-not-updateable"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance is not updateable"))
			})
		})

		Context("when an unexpected error occurs", func() {
			BeforeEach(func() {
				fakeServiceBroker.UpdateError = errors.New("broker failed")
			})

			It("returns a 500", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{"description":"broker failed"}`))
			})

			It("logs an appropriate error", func() {
				makeUpdateRequest(updateInstanceID, updateDetails, updateAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("update.unknown-error"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("broker failed"))
			})
		})

		Context("when we send invalid json", func() {
			makeBadUpdateRequest := func(instanceID string) *testflight.Response {
				response := &testflight.Response{}

				testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
					path := fmt.Sprintf("/v2/service_instances/%s", instanceID)

					body := strings.NewReader("{{{{{")
					request, err := http.NewRequest("PATCH", path, body)
					Expect(err).NotTo(HaveOccurred())
					request.Header.Add("Content-Type", "application/json")
					request.SetBasicAuth(credentials.Username, credentials.Password)

					response = r.Do(request)
				})

				return response
			}

			It("returns a 400 bad request", func() {
				response := makeBadUpdateRequest(updateInstanceID)
				Expect(response.StatusCode).Should(Equal(400))
			})

			It("logs a message", func() {
				makeBadUpdateRequest(updateInstanceID)
				Expect(lastLogLine().Message).To(ContainSubstring("update.invalid-update-details"))
			})
		})
	})

	Describe("deprovision", func() {
		var deprovisionInstanceID string
		var deprovisionServiceID string
		var deprovisionPlanID string
		var deprovisionDetails DeprovisionDetails
		var deprovisionAcceptsIncomplete bool

		makeDeprovisionRequest := func(instanceID string, serviceID string, planID string, acceptsIncomplete bool) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				path := fmt.Sprintf("/v2/service_instances/%s?service_id=%s&plan_id=%s", instanceID, serviceID, planID)
				if acceptsIncomplete {
					path = path + "&accepts_incomplete=true"
				}

				buffer := &bytes.Buffer{}
				json.NewEncoder(buffer).Encode(deprovisionDetails)
				request, err := http.NewRequest("DELETE", path, buffer)
				Expect(err).NotTo(HaveOccurred())
				request.Header.Add("Content-Type", "application/json")
				request.SetBasicAuth(credentials.Username, credentials.Password)

				response = r.Do(request)
			})
			return response
		}

		BeforeEach(func() {
			deprovisionInstanceID = uniqueInstanceID()
			deprovisionServiceID = "service-id"
			deprovisionPlanID = "plan-id"
			deprovisionDetails = DeprovisionDetails{
				ServiceID: deprovisionServiceID,
				PlanID:    deprovisionPlanID,
			}
			deprovisionAcceptsIncomplete = true

			fakeServiceBroker.DeprovisionAsynch = false
			fakeServiceBroker.DeprovisionError = nil
		})

		It("calls Deprovision on the service broker with the instance id", func() {
			makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
			Expect(fakeServiceBroker.DeprovisionInstanceID).To(Equal(deprovisionInstanceID))
		})

		It("calls Deprovision on the service broker with the deprovision details", func() {
			makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
			Expect(fakeServiceBroker.DeprovisionDetails).To(Equal(deprovisionDetails))
		})

		It("calls Deprovision on the service broker with accepts incomplete", func() {
			makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
			Expect(fakeServiceBroker.DeprovisionAcceptsIncomplete).To(Equal(deprovisionAcceptsIncomplete))
		})

		It("returns a 200", func() {
			response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
			Expect(response.StatusCode).To(Equal(200))
		})

		It("returns an empty JSON object", func() {
			response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
			Expect(response.Body).To(MatchJSON(`{}`))
		})

		Context("when broker is asynchronous", func() {
			BeforeEach(func() {
				fakeServiceBroker.DeprovisionAsynch = true
			})

			It("returns a 202", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(202))
			})

			It("returns an empty JSON object", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{}`))
			})
		})

		Context("when the instance does not exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.DeprovisionError = ErrInstanceDoesNotExist
			})

			It("returns a 410", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(410))
			})

			It("returns an empty JSON object", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{}`))
			})

			It("logs an appropriate error", func() {
				makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("deprovision.instance-missing"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance does not exist"))
			})
		})

		Context("when asyncronous operation is required", func() {
			BeforeEach(func() {
				fakeServiceBroker.DeprovisionError = ErrAsyncRequired
			})

			It("returns a 422", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(422))
			})

			It("returns proper json", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(fixture("instance_async_required.json")))
			})

			It("logs an appropriate error", func() {
				makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("deprovision.instance-async-required"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("This service plan requires client support for asynchronous service operations."))
			})
		})

		Context("when an unexpected error occurs", func() {
			BeforeEach(func() {
				fakeServiceBroker.DeprovisionError = errors.New("broker failed")
			})

			It("returns a 500", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(response.Body).To(MatchJSON(`{"description":"broker failed"}`))
			})

			It("logs an appropriate error", func() {
				makeDeprovisionRequest(deprovisionInstanceID, deprovisionServiceID, deprovisionPlanID, deprovisionAcceptsIncomplete)
				Expect(lastLogLine().Message).To(ContainSubstring("deprovision.unknown-error"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("broker failed"))
			})
		})
	})

	Describe("bind", func() {
		var bindInstanceID string
		var bindBindingID string
		var bindDetails BindDetails

		makeBindRequest := func(instanceID string, bindingID string, bindDetails BindDetails) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				path := fmt.Sprintf("/v2/service_instances/%s/service_bindings/%s", instanceID, bindingID)

				buffer := &bytes.Buffer{}
				json.NewEncoder(buffer).Encode(bindDetails)
				request, err := http.NewRequest("PUT", path, buffer)
				Expect(err).NotTo(HaveOccurred())
				request.Header.Add("Content-Type", "application/json")
				request.SetBasicAuth(credentials.Username, credentials.Password)

				response = r.Do(request)
			})
			return response
		}

		BeforeEach(func() {
			bindInstanceID = uniqueInstanceID()
			bindBindingID = uniqueBindingID()
			bindDetails = BindDetails{
				ServiceID: "service-id",
				PlanID:    "plan-id",
			}

			fakeServiceBroker.BindError = nil
			fakeServiceBroker.BindResponse = BindingResponse{
				Credentials: map[string]interface{}{
					"host":     "127.0.0.1",
					"port":     3000,
					"username": "batman",
					"password": "robin",
				},
			}
		})

		It("calls Bind on the service broker with the instance id", func() {
			makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
			Expect(fakeServiceBroker.BindInstanceID).To(Equal(bindInstanceID))
		})

		It("calls Bind on the service broker with the binding id", func() {
			makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
			Expect(fakeServiceBroker.BindBindingID).To(Equal(bindBindingID))
		})

		It("calls Bind on the service broker with the bind details", func() {
			makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
			Expect(fakeServiceBroker.BindDetails).To(Equal(bindDetails))
		})

		It("returns proper json", func() {
			response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
			Expect(response.Body).To(MatchJSON(fixture("binding.json")))
		})

		It("returns a 201", func() {
			response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
			Expect(response.StatusCode).To(Equal(201))
		})

		Context("when the instance does not exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.BindError = ErrInstanceDoesNotExist
			})

			It("returns a 500", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.Body).To(MatchJSON(`{"description":"instance does not exist"}`))
			})

			It("logs an appropriate error", func() {
				makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(lastLogLine().Message).To(ContainSubstring("bind.instance-missing"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance does not exist"))
			})
		})

		Context("when binding already exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.BindError = ErrBindingAlreadyExists
			})

			It("returns a 409", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.StatusCode).To(Equal(409))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.Body).To(MatchJSON(`{"description":"binding already exists"}`))
			})

			It("logs an appropriate error", func() {
				makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(lastLogLine().Message).To(ContainSubstring("bind.binding-already-exists"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("binding already exists"))
			})
		})

		Context("when app guid is required", func() {
			BeforeEach(func() {
				fakeServiceBroker.BindError = ErrAppGUIDRequired
			})

			It("returns a 422", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.StatusCode).To(Equal(422))
			})

			It("returns proper json", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.Body).To(MatchJSON(fixture("binding_app_required.json")))
			})

			It("logs an appropriate error", func() {
				makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(lastLogLine().Message).To(ContainSubstring("bind.binding-app-guid-required"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("This service supports generation of credentials through binding an application only."))
			})
		})

		Context("when instance is not bindable", func() {
			BeforeEach(func() {
				fakeServiceBroker.BindError = ErrInstanceNotBindable
			})

			It("returns a 500", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.Body).To(MatchJSON(`{"description":"instance is not bindable"}`))
			})

			It("logs an appropriate error", func() {
				makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(lastLogLine().Message).To(ContainSubstring("bind.instance-not-bindable"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance is not bindable"))
			})
		})

		Context("when an unexpected error occurs", func() {
			BeforeEach(func() {
				fakeServiceBroker.BindError = errors.New("broker failed")
			})

			It("returns a 500", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(response.Body).To(MatchJSON(`{"description":"broker failed"}`))
			})

			It("logs an appropriate error", func() {
				makeBindRequest(bindInstanceID, bindBindingID, bindDetails)
				Expect(lastLogLine().Message).To(ContainSubstring("bind.unknown-error"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("broker failed"))
			})
		})

		Context("when we send invalid json", func() {
			makeBadBindRequest := func(instanceID string, bindingID string) *testflight.Response {
				response := &testflight.Response{}

				testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
					path := fmt.Sprintf("/v2/service_instances/%s/service_bindings/%s", instanceID, bindingID)

					body := strings.NewReader("{{{{{")
					request, err := http.NewRequest("PUT", path, body)
					Expect(err).NotTo(HaveOccurred())
					request.Header.Add("Content-Type", "application/json")
					request.SetBasicAuth(credentials.Username, credentials.Password)

					response = r.Do(request)
				})

				return response
			}

			It("returns a 400 bad request", func() {
				response := makeBadBindRequest(bindInstanceID, bindBindingID)
				Expect(response.StatusCode).Should(Equal(400))
			})

			It("logs a message", func() {
				makeBadBindRequest(bindInstanceID, bindBindingID)
				Expect(lastLogLine().Message).To(ContainSubstring("bind.invalid-bind-details"))
			})
		})
	})

	Describe("unbind", func() {
		var unbindInstanceID string
		var unbindBindingID string
		var unbindServiceID string
		var unbindPlanID string
		var unbindDetails UnbindDetails

		makeUnbindRequest := func(instanceID string, bindingID string, serviceID string, planID string) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				path := fmt.Sprintf("/v2/service_instances/%s/service_bindings/%s?service_id=%s&plan_id=%s", instanceID, bindingID, serviceID, planID)

				buffer := &bytes.Buffer{}
				json.NewEncoder(buffer).Encode(unbindDetails)
				request, err := http.NewRequest("DELETE", path, buffer)
				Expect(err).NotTo(HaveOccurred())
				request.Header.Add("Content-Type", "application/json")
				request.SetBasicAuth(credentials.Username, credentials.Password)

				response = r.Do(request)
			})
			return response
		}

		BeforeEach(func() {
			unbindInstanceID = uniqueInstanceID()
			unbindBindingID = uniqueBindingID()
			unbindServiceID = "service-id"
			unbindPlanID = "plan-id"
			unbindDetails = UnbindDetails{
				ServiceID: unbindServiceID,
				PlanID:    unbindPlanID,
			}

			fakeServiceBroker.UnbindError = nil
		})

		It("calls Unbind on the service broker with the instance id", func() {
			makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
			Expect(fakeServiceBroker.UnbindInstanceID).To(Equal(unbindInstanceID))
		})

		It("calls Unbind on the service broker with the binding id", func() {
			makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
			Expect(fakeServiceBroker.UnbindBindingID).To(Equal(unbindBindingID))
		})

		It("calls Unbind on the service broker with the bind details", func() {
			makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
			Expect(fakeServiceBroker.UnbindDetails).To(Equal(unbindDetails))
		})

		It("returns an empty JSON object", func() {
			response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
			Expect(response.Body).To(MatchJSON(`{}`))
		})

		It("returns a 200", func() {
			response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
			Expect(response.StatusCode).To(Equal(200))
		})

		Context("when the instance does not exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.UnbindError = ErrInstanceDoesNotExist
			})

			It("returns a 500", func() {
				response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(response.Body).To(MatchJSON(`{"description":"instance does not exist"}`))
			})

			It("logs an appropriate error", func() {
				makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(lastLogLine().Message).To(ContainSubstring("unbind.instance-missing"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance does not exist"))
			})
		})

		Context("when binding does not exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.UnbindError = ErrBindingDoesNotExist
			})

			It("returns a 410", func() {
				response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(response.StatusCode).To(Equal(410))
			})

			It("returns an empty JSON object", func() {
				response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(response.Body).To(MatchJSON(`{}`))
			})

			It("logs an appropriate error", func() {
				makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(lastLogLine().Message).To(ContainSubstring("unbind.binding-missing"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("binding does not exist"))
			})
		})

		Context("when an unexpected error occurs", func() {
			BeforeEach(func() {
				fakeServiceBroker.UnbindError = errors.New("broker failed")
			})

			It("returns a 500", func() {
				response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(response.Body).To(MatchJSON(`{"description":"broker failed"}`))
			})

			It("logs an appropriate error", func() {
				makeUnbindRequest(unbindInstanceID, unbindBindingID, unbindServiceID, unbindPlanID)
				Expect(lastLogLine().Message).To(ContainSubstring("unbind.unknown-error"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("broker failed"))
			})
		})
	})

	Describe("last operation", func() {
		var lastOperationInstanceID string

		makeLastOperationRequest := func(instanceID string) *testflight.Response {
			response := &testflight.Response{}
			testflight.WithServer(brokerAPI, func(r *testflight.Requester) {
				path := fmt.Sprintf("/v2/service_instances/%s/last_operation", instanceID)

				request, err := http.NewRequest("GET", path, strings.NewReader(""))
				Expect(err).NotTo(HaveOccurred())
				request.Header.Add("Content-Type", "application/json")
				request.SetBasicAuth(credentials.Username, credentials.Password)

				response = r.Do(request)
			})
			return response
		}

		BeforeEach(func() {
			lastOperationInstanceID = uniqueInstanceID()

			fakeServiceBroker.LastOperationResponse = LastOperationResponse{
				State: "succeeded",
			}
			fakeServiceBroker.LastOperationError = nil
		})

		It("calls LastOperation on the service broker with the instance id", func() {
			makeLastOperationRequest(lastOperationInstanceID)
			Expect(fakeServiceBroker.LastOperationInstanceID).To(Equal(lastOperationInstanceID))
		})

		It("returns a 200", func() {
			response := makeLastOperationRequest(lastOperationInstanceID)
			Expect(response.StatusCode).To(Equal(200))
		})

		It("returns proper json", func() {
			response := makeLastOperationRequest(lastOperationInstanceID)
			Expect(response.Body).To(MatchJSON(fixture("last_operation.json")))
		})

		Context("when broker returns a description field", func() {
			BeforeEach(func() {
				fakeServiceBroker.LastOperationResponse = LastOperationResponse{
					State:       "succeeded",
					Description: "Progress: 100%",
				}
			})

			It("returns proper json", func() {
				response := makeLastOperationRequest(lastOperationInstanceID)
				Expect(response.Body).To(MatchJSON(fixture("last_operation_description.json")))
			})
		})

		Context("when the instance does not exists", func() {
			BeforeEach(func() {
				fakeServiceBroker.LastOperationError = ErrInstanceDoesNotExist
			})

			It("returns a 410", func() {
				response := makeLastOperationRequest(lastOperationInstanceID)
				Expect(response.StatusCode).To(Equal(410))
			})

			It("returns an empty JSON object", func() {
				response := makeLastOperationRequest(lastOperationInstanceID)
				Expect(response.Body).To(MatchJSON(`{}`))
			})

			It("logs an appropriate error", func() {
				makeLastOperationRequest(lastOperationInstanceID)
				Expect(lastLogLine().Message).To(ContainSubstring("last-operation.instance-missing"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("instance does not exist"))
			})
		})

		Context("when an unexpected error occurs", func() {
			BeforeEach(func() {
				fakeServiceBroker.LastOperationError = errors.New("broker failed")
			})

			It("returns a 500", func() {
				response := makeLastOperationRequest(lastOperationInstanceID)
				Expect(response.StatusCode).To(Equal(500))
			})

			It("returns json with a description field and a useful error message", func() {
				response := makeLastOperationRequest(lastOperationInstanceID)
				Expect(response.Body).To(MatchJSON(`{"description":"broker failed"}`))
			})

			It("logs an appropriate error", func() {
				makeLastOperationRequest(lastOperationInstanceID)
				Expect(lastLogLine().Message).To(ContainSubstring("last-operation.unknown-error"))
				Expect(lastLogLine().Data["error"]).To(ContainSubstring("broker failed"))
			})
		})
	})
})
