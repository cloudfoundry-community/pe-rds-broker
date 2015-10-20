package database_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cf-platform-eng/rds-broker/database"

	"github.com/pivotal-golang/lager"
)

var _ = Describe("Provider Service", func() {
	var (
		dbProvider *ProviderService
		logger     lager.Logger
	)

	BeforeEach(func() {
		dbProvider = NewProviderService()
		logger = lager.NewLogger("database_test")
	})

	Describe("GetDatabase", func() {
		It("returns error if database engine is not supported", func() {
			_, err := dbProvider.GetDatabase("unknown", logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Database 'unknown' not supported"))
		})

		Context("when database engine is aurora", func() {
			It("return the proper Database", func() {
				database, err := dbProvider.GetDatabase("aurora", logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(database).To(BeAssignableToTypeOf(&MySQLDatabase{}))
			})
		})

		Context("when database engine is mariadb", func() {
			It("return the proper Database", func() {
				database, err := dbProvider.GetDatabase("mariadb", logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(database).To(BeAssignableToTypeOf(&MySQLDatabase{}))
			})
		})

		Context("when database engine is mysql", func() {
			It("return the proper Database", func() {
				database, err := dbProvider.GetDatabase("mysql", logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(database).To(BeAssignableToTypeOf(&MySQLDatabase{}))
			})
		})

		Context("when database engine is postgres", func() {
			It("return the proper Database", func() {
				database, err := dbProvider.GetDatabase("postgres", logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(database).To(BeAssignableToTypeOf(&PostgresDatabase{}))
			})
		})

		Context("when database engine is postgresql", func() {
			It("return the proper Database", func() {
				database, err := dbProvider.GetDatabase("postgresql", logger)
				Expect(err).ToNot(HaveOccurred())
				Expect(database).To(BeAssignableToTypeOf(&PostgresDatabase{}))
			})
		})
	})
})
