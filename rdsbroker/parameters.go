package rdsbroker

type ProvisionParameters struct {
	BackupRetentionPeriod      int64  `json:"backup_retention_period"`
	CharacterSetName           string `json:"character_set_name"`
	DBName                     string `json:"dbname"`
	PreferredBackupWindow      string `json:"preferred_backup_window"`
	PreferredMaintenanceWindow string `json:"preferred_maintenance_window"`
}

type UpdateParameters struct {
	ApplyImmediately           bool   `json:"apply_immediately"`
	BackupRetentionPeriod      int64  `json:"backup_retention_period"`
	PreferredBackupWindow      string `json:"preferred_backup_window"`
	PreferredMaintenanceWindow string `json:"preferred_maintenance_window"`
}

type BindParameters struct {
	DBName string `json:"dbname"`
}

type CredentialsHash struct {
	Host     string `json:"host,omitempty"`
	Port     int64  `json:"port,omitempty"`
	Name     string `json:"name,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	URI      string `json:"uri,omitempty"`
	JDBCURI  string `json:"jdbcUrl,omitempty"`
}
