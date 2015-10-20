package fakes

type FakeDatabase struct {
	OpenCalled   bool
	OpenAddress  string
	OpenPort     int64
	OpenName     string
	OpenUsername string
	OpenPassword string
	OpenError    error

	CloseCalled bool

	ExistsCalled bool
	ExistsName   string
	ExistsError  error

	CreateCalled bool
	CreateName   string
	CreateError  error

	DropCalled bool
	DropName   string
	DropError  error

	CreateUserCalled   bool
	CreateUserUsername string
	CreateUserPassword string
	CreateUserError    error

	DropUserCalled   bool
	DropUserUsername string
	DropUserError    error

	PrivilegesCalled     bool
	PrivilegesPrivileges map[string][]string
	PrivilegesError      error

	GrantPrivilegesCalled   bool
	GrantPrivilegesName     string
	GrantPrivilegesUsername string
	GrantPrivilegesError    error

	RevokePrivilegesCalled   bool
	RevokePrivilegesName     string
	RevokePrivilegesUsername string
	RevokePrivilegesError    error
}

func (f *FakeDatabase) Open(address string, port int64, name string, username string, password string) error {
	f.OpenCalled = true
	f.OpenAddress = address
	f.OpenPort = port
	f.OpenName = name
	f.OpenUsername = username
	f.OpenPassword = password

	return f.OpenError
}

func (f *FakeDatabase) Close() {
	f.CloseCalled = true
}

func (f *FakeDatabase) Exists(name string) (bool, error) {
	f.ExistsCalled = true
	f.ExistsName = name

	return true, f.ExistsError
}

func (f *FakeDatabase) Create(name string) error {
	f.CreateCalled = true
	f.CreateName = name

	return f.CreateError
}

func (f *FakeDatabase) Drop(name string) error {
	f.DropCalled = true
	f.DropName = name

	return f.DropError
}

func (f *FakeDatabase) CreateUser(username string, password string) error {
	f.CreateUserCalled = true
	f.CreateUserUsername = username
	f.CreateUserPassword = password

	return f.CreateUserError
}

func (f *FakeDatabase) DropUser(username string) error {
	f.DropUserCalled = true
	f.DropUserUsername = username

	return f.DropUserError
}

func (f *FakeDatabase) Privileges() (map[string][]string, error) {
	f.PrivilegesCalled = true

	return f.PrivilegesPrivileges, f.PrivilegesError
}

func (f *FakeDatabase) GrantPrivileges(name string, username string) error {
	f.GrantPrivilegesCalled = true
	f.GrantPrivilegesName = name
	f.GrantPrivilegesUsername = username

	return f.GrantPrivilegesError
}

func (f *FakeDatabase) RevokePrivileges(name string, username string) error {
	f.RevokePrivilegesCalled = true
	f.RevokePrivilegesName = name
	f.RevokePrivilegesUsername = username

	return f.RevokePrivilegesError
}
