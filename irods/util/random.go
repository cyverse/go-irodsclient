package util

import (
	"math/rand"
	"time"
)

var (
	letters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

// MakeRandomString returns a random string
func MakeRandomString(size int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]rune, size)
	for i := 0; i < size; i++ {
		b[i] = letters[r.Intn(len(letters))]
	}

	bs := string(b)
	return bs
}
