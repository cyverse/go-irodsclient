package testcases

import "testing"

func FailError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%+v", err)
		t.FailNow()
	}
}
