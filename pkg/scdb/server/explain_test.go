package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsExplain(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
		hasError bool
	}{
		{
			name:     "Valid EXPLAIN query",
			query:    "EXPLAIN SELECT * FROM users",
			expected: true,
			hasError: false,
		},
		{
			name:     "Valid EXPLAIN query in lowercase",
			query:    "explain select * from users",
			expected: true,
			hasError: false,
		},
		{
			name:     "Valid EXPLAIN query with whitespace",
			query:    "  EXPLAIN   SELECT * FROM users  ",
			expected: true,
			hasError: false,
		},
		{
			name:     "Non-EXPLAIN query",
			query:    "SELECT * FROM users",
			expected: false,
			hasError: false,
		},
		{
			name:     "INSERT query",
			query:    "INSERT INTO users (name) VALUES ('alice')",
			expected: false,
			hasError: false,
		},
		{
			name:     "Empty query",
			query:    "",
			expected: true,
			hasError: true,
		},
		{
			name:     "EXPLAIN with complex query",
			query:    "EXPLAIN SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > 18",
			expected: true,
			hasError: false,
		},
		{
			name:     "EXPLAIN ANALYZE query",
			query:    "EXPLAIN ANALYZE SELECT * FROM users",
			expected: true,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := isExplainQuery(tt.query)

			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
