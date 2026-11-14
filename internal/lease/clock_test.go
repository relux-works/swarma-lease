package lease

import "testing"

// TestNormalizeBootSessionID canonicalizes uppercase platform UUIDs while accepting lowercase UUIDs and rejecting malformed values.
func TestNormalizeBootSessionID(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		valid bool
	}{
		{name: "uppercase platform UUID", input: "A1B2C3D4-E5F6-4789-ABCD-0123456789AB", want: "a1b2c3d4-e5f6-4789-abcd-0123456789ab", valid: true},
		{name: "canonical lowercase UUID", input: "a1b2c3d4-e5f6-4789-abcd-0123456789ab", want: "a1b2c3d4-e5f6-4789-abcd-0123456789ab", valid: true},
		{name: "malformed UUID", input: "A1B2-not-a-uuid", valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, valid := normalizeBootSessionID(test.input)
			if valid != test.valid || got != test.want {
				t.Fatalf("normalizeBootSessionID(%q) = (%q, %t), want (%q, %t)", test.input, got, valid, test.want, test.valid)
			}
		})
	}
}
