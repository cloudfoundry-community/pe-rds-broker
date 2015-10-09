package utils

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"io"
	"math"
)

var alpha = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
var numer = []byte("0123456789")

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

func GetMD5B64(text string, length int) string {
	hasher := md5.New()
	md5 := hasher.Sum([]byte(text))
	return base64.StdEncoding.EncodeToString(md5)[0:int(math.Min(float64(length), float64(len(md5))))]
}
