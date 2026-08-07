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
	assert.NoError(t, err, "YAML marshal of duration should succeed")
	assert.NotEmpty(t, string(yamlBytes), "marshaled duration should produce non-empty bytes")

	var d2 types.Duration
	err = yaml.Unmarshal(yamlBytes, &d2)
	assert.NoError(t, err, "YAML unmarshal of marshaled duration should succeed")
	assert.Equal(t, d1, d2, "unmarshaled duration should equal original duration")
}

func testDurationUnmarshalFromYAMLWithoutUnits(t *testing.T) {
	v1 := []byte("60000000000")

	var d1 types.Duration
	err := yaml.Unmarshal(v1, &d1)
	assert.NoError(t, err, "unmarshal raw nanoseconds should succeed")
	assert.Equal(t, 1*time.Minute, time.Duration(d1), "60000000000 nanoseconds should parse to 1 minute")

	v2 := []byte("6h60000000000")

	var d2 types.Duration
	err = yaml.Unmarshal(v2, &d2)
	assert.NoError(t, err, "unmarshal unit+nanoseconds mix should succeed")
	assert.Equal(t, 6*time.Hour+1*time.Minute, time.Duration(d2), "mixed unit string should sum components correctly")
}

func testDurationUnmarshalFromYAML(t *testing.T) {
	v1 := []byte("6m")

	var d1 types.Duration
	err := yaml.Unmarshal(v1, &d1)
	assert.NoError(t, err, "unmarshal minute unit string should succeed")
	assert.Equal(t, 6*time.Minute, time.Duration(d1), "6m string should parse to 6 minutes")

	v2 := []byte("6h6m")

	var d2 types.Duration
	err = yaml.Unmarshal(v2, &d2)
	assert.NoError(t, err, "unmarshal combined hour+minute string should succeed")
	assert.Equal(t, 6*time.Hour+6*time.Minute, time.Duration(d2), "6h6m string should sum both units correctly")
}
