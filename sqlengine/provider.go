package sqlengine

// Provider Interface
type Provider interface {
	GetSQLEngine(engine string) (SQLEngine, error)
}
