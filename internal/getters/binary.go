package getters

import (
	"fmt"
	"math/rand"
)

// RandomString getter
type RandomBinary struct {
	name      string
	maxSize   int64
	allowNull bool
}

func (r *RandomBinary) Value() interface{} {
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}
	maxSize := uint64(r.maxSize)
	if maxSize == 0 {
		maxSize = uint64(rand.Int63n(100))
	}

	data := make([]byte, maxSize)
	rand.Read(data)

	return data
}

func (r *RandomBinary) String() string {
	v := r.Value()
	if v == nil {
		return NULL
	}
	return v.(string)
}

func (r *RandomBinary) Quote() string {
	v := r.Value()
	if v == nil {
		return NULL
	}
	return fmt.Sprintf("%q", v)
}

func NewRandomBinary(name string, maxSize int64, allowNull bool) *RandomBinary {
	return &RandomBinary{name, maxSize, allowNull}
}
