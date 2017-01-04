package utils

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"math"
)

var alpha = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
var numer = []byte("0123456789")

// RandomAlphaNum generate random alpha number with specific length
func RandomAlphaNum(length int) string {
	return randChar(1, alpha) + randChar(length-1, append(alpha, numer...))
}

func randChar(length int, chars []byte) string {
	newPword := make([]byte, length)
	randomData := make([]byte, length+(length/4))
	clen := byte(len(chars))
	maxrb := byte(256 - (256 % len(chars)))
	i := 0
	for {
		if _, err := io.ReadFull(rand.Reader, randomData); err != nil {
			panic(err)
		}
		for _, c := range randomData {
			if c >= maxrb {
				continue
			}
			newPword[i] = chars[c%clen]
			i++
			if i == length {
				return string(newPword)
			}
		}
	}
}

// GetSHA256B64 get base64 encoding of sting SHA2 hash and add a salt as optional parameter
func GetSHA256B64(text string, length int, params ...string) string {
	if len(params) > 0 && params[0] != "" {
		text = saltText(text, params[0])
	}

	hasher := sha256.New()
	hasher.Write([]byte(text))
	sha := hasher.Sum(nil)
	return base64.StdEncoding.EncodeToString(sha)[0:int(math.Min(float64(length), float64(len(sha))))]
}

// GetMD5B64 get base64 encoding of string add a salt as optional parameter
func GetMD5B64(text string, length int, params ...string) string {
	if len(params) > 0 && params[0] != "" {
		text = saltText(text, params[0])
	}
	hasher := md5.New()
	md5 := hasher.Sum([]byte(text))
	return base64.StdEncoding.EncodeToString(md5)[0:int(math.Min(float64(length), float64(len(md5))))]
}

func saltText(text string, salt string) string {
	var b bytes.Buffer
	i := 0
	for _, c := range text {
		if i >= len(salt) {
			i = 0
		}

		b.WriteByte(byte(c))
		b.WriteByte(salt[i])
		i++
	}

	return b.String()
}
