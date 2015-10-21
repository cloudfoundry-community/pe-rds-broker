package database

type Database interface {
	URI(address string, port int64, name string, username string, password string) string
	JDBCURI(address string, port int64, name string, username string, password string) string
	Open(address string, port int64, name string, username string, password string) error
	Close()
	Exists(name string) (bool, error)
	Create(name string) error
	Drop(name string) error
	CreateUser(username string, password string) error
	DropUser(username string) error
	Privileges() (map[string][]string, error)
	GrantPrivileges(name string, username string) error
	RevokePrivileges(name string, username string) error
}
