package rdsbroker

import (
	"context"
	"encoding/json"

	"github.com/pivotal-cf/brokerapi"
)

// UpdateServices is appliying changes to the plan config on all instances.
func UpdateServices(b *RDSBroker) error {
	context := context.Background()

	parameters := UpdateParameters{
		ApplyImmediately: true,
	}

	parametersJSON, _ := json.Marshal(parameters)

	update := func(instanceID string, details ServiceDetails) error {

		d := brokerapi.UpdateDetails{
			ServiceID:     details.ServiceID,
			PlanID:        details.PlanID,
			RawParameters: parametersJSON,
			PreviousValues: brokerapi.PreviousValues{
				PlanID:    details.PlanID,
				ServiceID: details.ServiceID,
				OrgID:     details.OrgID,
				SpaceID:   details.SpaceID,
			},
		}
		_, err := b.Update(context, instanceID, d, true)

		return err
	}

	err := b.BulkUpdate(context, update)
	return err
}
