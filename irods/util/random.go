package util

import (
	"math/rand"
	"time"
)

var (
	letters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

//MakeRandomString returns a random string
func MakeRandomString(size int) string {
	rand.Seed(time.Now().UnixNano())

	b := make([]rune, size)
	for i := 0; i < size; i++ {
		b[i] = letters[rand.Intn(len(letters))]
	}

	bs := string(b)
	return bs
}
