package sqlengine

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL Driver

	"github.com/pivotal-golang/lager"
)

type PostgresEngine struct {
	logger lager.Logger
	db     *sql.DB
}

func NewPostgresEngine(logger lager.Logger) *PostgresEngine {
	return &PostgresEngine{
		logger: logger.Session("postgres-engine"),
	}
}

func (d *PostgresEngine) Open(address string, port int64, dbname string, username string, password string) error {
	connectionString := d.connectionString(address, port, dbname, username, password)
	d.logger.Debug("sql-open", lager.Data{"connection-string": connectionString})

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	d.db = db

	return nil
}

func (d *PostgresEngine) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *PostgresEngine) ExistsDB(dbname string) (bool, error) {
	selectDatabaseStatement := "SELECT datname FROM pg_database WHERE datname='" + dbname + "'"
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

func (d *PostgresEngine) CreateDB(dbname string) error {
	ok, err := d.ExistsDB(dbname)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	createDBStatement := "CREATE DATABASE \"" + dbname + "\""
	d.logger.Debug("create-database", lager.Data{"statement": createDBStatement})

	if _, err := d.db.Exec(createDBStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) DropDB(dbname string) error {
	if err := d.dropConnections(dbname); err != nil {
		return err
	}

	dropDBStatement := "DROP DATABASE IF EXISTS \"" + dbname + "\""
	d.logger.Debug("drop-database", lager.Data{"statement": dropDBStatement})

	if _, err := d.db.Exec(dropDBStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) CreateUser(username string, password string) error {
	createUserStatement := "CREATE USER \"" + username + "\" WITH PASSWORD '" + password + "'"
	d.logger.Debug("create-user", lager.Data{"statement": createUserStatement})

	if _, err := d.db.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) DropUser(username string) error {
	// For PostgreSQL we don't drop the user because it might still be owner of some objects

	return nil
}

func (d *PostgresEngine) Privileges() (map[string][]string, error) {
	privileges := make(map[string][]string)

	selectPrivilegesStatement := "SELECT datname, usename FROM pg_database d, pg_user u WHERE usecreatedb = false AND (SELECT has_database_privilege(u.usename, d.datname, 'create'))"
	d.logger.Debug("database-privileges", lager.Data{"statement": selectPrivilegesStatement})

	rows, err := d.db.Query(selectPrivilegesStatement)
	if err != nil {
		d.logger.Error("sql-error", err)
		return privileges, err
	}
	defer rows.Close()

	var dbname, username string
	for rows.Next() {
		err := rows.Scan(&dbname, &username)
		if err != nil {
			d.logger.Error("sql-error", err)
			return privileges, err
		}
		if _, ok := privileges[dbname]; !ok {
			privileges[dbname] = []string{}
		}
		privileges[dbname] = append(privileges[dbname], username)
	}
	err = rows.Err()
	if err != nil {
		d.logger.Error("sql-error", err)
		return privileges, err
	}

	d.logger.Debug("database-privileges", lager.Data{"output": privileges})

	return privileges, nil
}

func (d *PostgresEngine) GrantPrivileges(dbname string, username string) error {
	grantPrivilegesStatement := "GRANT ALL PRIVILEGES ON DATABASE \"" + dbname + "\" TO \"" + username + "\""
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.db.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) RevokePrivileges(dbname string, username string) error {
	revokePrivilegesStatement := "REVOKE ALL PRIVILEGES ON DATABASE \"" + dbname + "\" FROM \"" + username + "\""
	d.logger.Debug("revoke-privileges", lager.Data{"statement": revokePrivilegesStatement})

	if _, err := d.db.Exec(revokePrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) URI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?reconnect=true", username, password, address, port, dbname)
}

func (d *PostgresEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}

func (d *PostgresEngine) dropConnections(dbname string) error {
	dropDBConnectionsStatement := "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + dbname + "' AND pid <> pg_backend_pid()"
	d.logger.Debug("drop-connections", lager.Data{"statement": dropDBConnectionsStatement})

	if _, err := d.db.Exec(dropDBConnectionsStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) connectionString(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user='%s' password='%s'", address, port, dbname, username, password)
}
