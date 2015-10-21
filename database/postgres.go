package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL Driver

	"github.com/pivotal-golang/lager"
)

type PostgresDatabase struct {
	logger lager.Logger
	db     *sql.DB
}

func NewPostgresDatabase(logger lager.Logger) *PostgresDatabase {
	return &PostgresDatabase{
		logger: logger.Session("postgres-database"),
	}
}

func (d *PostgresDatabase) URI(address string, port int64, name string, username string, password string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?reconnect=true", username, password, address, port, name)
}

func (d *PostgresDatabase) JDBCURI(address string, port int64, name string, username string, password string) string {
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?user=%s&password=%s", address, port, name, username, password)
}

func (d *PostgresDatabase) Open(address string, port int64, name string, username string, password string) error {
	connectionString := d.connectionString(address, port, name, username, password)
	d.logger.Debug("sql-open", lager.Data{"connection-string": connectionString})

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	d.db = db

	return nil
}

func (d *PostgresDatabase) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *PostgresDatabase) Exists(name string) (bool, error) {
	selectDatabaseStatement := "SELECT datname FROM pg_database WHERE datname='" + name + "'"
	d.logger.Debug("database-exists", lager.Data{"statement": selectDatabaseStatement})

	var dummy string
	err := d.db.QueryRow(selectDatabaseStatement).Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func (d *PostgresDatabase) Create(name string) error {
	ok, err := d.Exists(name)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	createDBStatement := "CREATE DATABASE \"" + name + "\""
	d.logger.Debug("create-database", lager.Data{"statement": createDBStatement})

	if _, err := d.db.Exec(createDBStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresDatabase) Drop(name string) error {
	if err := d.dropConnections(name); err != nil {
		return err
	}

	dropDBStatement := "DROP DATABASE IF EXISTS \"" + name + "\""
	d.logger.Debug("drop-database", lager.Data{"statement": dropDBStatement})

	if _, err := d.db.Exec(dropDBStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresDatabase) CreateUser(username string, password string) error {
	createUserStatement := "CREATE USER \"" + username + "\" WITH PASSWORD '" + password + "'"
	d.logger.Debug("create-user", lager.Data{"statement": createUserStatement})

	if _, err := d.db.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresDatabase) DropUser(username string) error {
	// For PostgreSQL we don't drop the user because it might still be owner of some objects

	return nil
}

func (d *PostgresDatabase) Privileges() (map[string][]string, error) {
	privileges := make(map[string][]string)

	selectPrivilegesStatement := "SELECT datname, usename FROM pg_database d, pg_user u WHERE usecreatedb = false AND (SELECT has_database_privilege(u.usename, d.datname, 'create'))"
	d.logger.Debug("database-privileges", lager.Data{"statement": selectPrivilegesStatement})

	rows, err := d.db.Query(selectPrivilegesStatement)
	defer rows.Close()

	var dbname, username string
	for rows.Next() {
		err := rows.Scan(&dbname, &username)
		if err != nil {
			return privileges, err
		}
		if _, ok := privileges[dbname]; !ok {
			privileges[dbname] = []string{}
		}
		privileges[dbname] = append(privileges[dbname], username)
	}
	err = rows.Err()
	if err != nil {
		return privileges, err
	}

	d.logger.Debug("database-privileges", lager.Data{"output": privileges})

	return privileges, nil
}

func (d *PostgresDatabase) GrantPrivileges(name string, username string) error {
	grantPrivilegesStatement := "GRANT ALL PRIVILEGES ON DATABASE \"" + name + "\" TO \"" + username + "\""
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.db.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresDatabase) RevokePrivileges(name string, username string) error {
	revokePrivilegesStatement := "REVOKE ALL PRIVILEGES ON DATABASE \"" + name + "\" FROM \"" + username + "\""
	d.logger.Debug("revoke-privileges", lager.Data{"statement": revokePrivilegesStatement})

	if _, err := d.db.Exec(revokePrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresDatabase) dropConnections(name string) error {
	dropDBConnectionsStatement := "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + name + "' AND pid <> pg_backend_pid()"
	d.logger.Debug("drop-connections", lager.Data{"statement": dropDBConnectionsStatement})

	if _, err := d.db.Exec(dropDBConnectionsStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresDatabase) connectionString(address string, port int64, name string, username string, password string) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user='%s' password='%s'", address, port, name, username, password)
}
