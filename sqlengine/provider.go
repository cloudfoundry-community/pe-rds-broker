package sqlengine

type Provider interface {
	GetSQLEngine(engine string) (SQLEngine, error)
}
