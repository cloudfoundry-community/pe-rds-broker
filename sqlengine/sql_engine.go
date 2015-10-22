package sqlengine

type SQLEngine interface {
	Open(address string, port int64, dbname string, username string, password string) error
	Close()
	ExistsDB(dbname string) (bool, error)
	CreateDB(dbname string) error
	DropDB(dbname string) error
	CreateUser(username string, password string) error
	DropUser(username string) error
	Privileges() (map[string][]string, error)
	GrantPrivileges(dbname string, username string) error
	RevokePrivileges(dbname string, username string) error
	URI(address string, port int64, dbname string, username string, password string) string
	JDBCURI(address string, port int64, dbname string, username string, password string) string
}
