package testcases

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func getTypeDurationTest() Test {
	return Test{
		Name: "Type_Duration",
		Func: typeDurationTest,
	}
}

func typeDurationTest(t *testing.T, test *Test) {
	t.Run("MarshalToYAML", testDurationMarshalToYAML)
	t.Run("UnmarshalFromYAMLWithoutUnits", testDurationUnmarshalFromYAMLWithoutUnits)
	t.Run("UnmarshalFromYAML", testDurationUnmarshalFromYAML)
}

func testDurationMarshalToYAML(t *testing.T) {
	d1 := types.Duration(5 * time.Minute)

	yamlBytes, err := yaml.Marshal(d1)
	assert.NoError(t, err)
	assert.NotEmpty(t, string(yamlBytes))

	var d2 types.Duration
	err = yaml.Unmarshal(yamlBytes, &d2)
	assert.NoError(t, err)
	assert.Equal(t, d1, d2)
}

func testDurationUnmarshalFromYAMLWithoutUnits(t *testing.T) {
	v1 := []byte("60000000000")

	var d1 types.Duration
	err := yaml.Unmarshal(v1, &d1)
	assert.NoError(t, err)
	assert.Equal(t, 1*time.Minute, time.Duration(d1))

	v2 := []byte("6h60000000000")

	var d2 types.Duration
	err = yaml.Unmarshal(v2, &d2)
	assert.NoError(t, err)
	assert.Equal(t, 6*time.Hour+1*time.Minute, time.Duration(d2))
}

func testDurationUnmarshalFromYAML(t *testing.T) {
	v1 := []byte("6m")

	var d1 types.Duration
	err := yaml.Unmarshal(v1, &d1)
	assert.NoError(t, err)
	assert.Equal(t, 6*time.Minute, time.Duration(d1))

	v2 := []byte("6h6m")

	var d2 types.Duration
	err = yaml.Unmarshal(v2, &d2)
	assert.NoError(t, err)
	assert.Equal(t, 6*time.Hour+6*time.Minute, time.Duration(d2))
}
