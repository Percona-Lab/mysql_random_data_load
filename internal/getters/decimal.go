package getters

import (
	"fmt"
	"math"
	"math/rand"
)

type RandomDecimal struct {
	name      string
	size      int64
	allowNull bool
}

func (r *RandomDecimal) Value() interface{} {
	f := rand.Float64() * float64(rand.Int63n(int64(math.Pow10(int(r.size)))))
	return f
}

func (r *RandomDecimal) String() string {
	return fmt.Sprintf("%0f", r.Value())
}

func NewRandomDecimal(name string, size int64, allowNull bool) Getter {
	return &RandomDecimal{name, size, allowNull}
}
