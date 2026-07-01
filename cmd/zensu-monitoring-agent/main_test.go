package main

import "testing"

func TestEnvBool(t *testing.T) {
	cases := []struct {
		val  string
		def  bool
		want bool
	}{
		{"", true, true},
		{"", false, false},
		{"1", false, true},
		{"true", false, true},
		{"TRUE", false, true},
		{"yes", false, true},
		{"on", false, true},
		{"  true  ", false, true},
		{"0", true, false},
		{"false", true, false},
		{"off", true, false},
		{"garbage", true, false},
	}
	for _, c := range cases {
		t.Setenv("ZENSU_MONITORING_AGENT_TEST_BOOL", c.val)
		if got := envBool("ZENSU_MONITORING_AGENT_TEST_BOOL", c.def); got != c.want {
			t.Errorf("envBool(val=%q, def=%v) = %v, want %v", c.val, c.def, got, c.want)
		}
	}
}
