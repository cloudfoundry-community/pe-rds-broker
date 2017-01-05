package utils_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-community/pe-rds-broker/utils"
)

var _ = Describe("RandomAlphaNum", func() {
	It("generates a random alpha numeric with the proper length", func() {
		randomString := RandomAlphaNum(32)
		Expect(len(randomString)).To(Equal(32))
	})
})

var _ = Describe("GetMD5B64", func() {
	It("returns the Base64 of a string MD5", func() {
		md5b64 := GetMD5B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32)
		Expect(md5b64).To(Equal("Y2U3MWI0ODQtZDU0Mi00MGY3LTlkZDQt"))
	})

	It("returns the Base64 of a string MD5 with empty salt", func() {
		md5b64 := GetMD5B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32, "")
		Expect(md5b64).To(Equal("Y2U3MWI0ODQtZDU0Mi00MGY3LTlkZDQt"))
	})

	It("returns the Base64 of a string MD5 including random numbers", func() {
		md5b64 := GetMD5B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32, "Y2U3MWI0ODQtZDU0Mi00MGY3LTlkZDQt")
		Expect(md5b64).To(Equal("Y1llMjdVMTNiTTRXOEk0MC1PZEQ1UTR0"))
	})
})
var _ = Describe("GetSHA256B64", func() {
	It("returns the Base64 of a string SHA256", func() {
		sha256b64 := GetSHA256B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32)
		Expect(sha256b64).To(Equal("BJ3IzLRK6pmhB98A1S7RmgWkkgmK1MSQ"))
	})

	It("returns the Base64 of a string SHA256 with empty salt", func() {
		sha256b64 := GetSHA256B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32, "")
		Expect(sha256b64).To(Equal("BJ3IzLRK6pmhB98A1S7RmgWkkgmK1MSQ"))
	})

	It("returns the Base64 of a string SHA256 including random numbers", func() {
		sha256b64 := GetSHA256B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32, "Y2U3MWI0ODQtZDU0Mi00MGY3LTlkZDQt")
		Expect(sha256b64).To(Equal("i7ewNrLEglx-z0sDbKmM_dfhU9VQtkZk"))
	})
})
