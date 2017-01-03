package rdsbroker

import (
	"context"

	"github.com/cloudfoundry-community/pe-rds-broker/awsrds"
)

// UpdatePasswords based on configuration
func UpdatePasswords(b *RDSBroker) error {
	context := context.Background()

	updateCluster := func(instanceID string, cluster awsrds.DBClusterDetails) error {
		cluster.MasterUserPassword = b.masterPassword(instanceID)

		err := b.dbCluster.Modify(cluster.Identifier, cluster, true)
		return err
	}

	updateInstance := func(instanceID string, instance awsrds.DBInstanceDetails) error {
		instance.MasterUserPassword = b.masterPassword(instanceID)

		err := b.dbInstance.Modify(instance.Identifier, instance, true)
		return err

	}

	err := b.BulkUpdate(context, updateCluster, updateInstance)
	return err
}
