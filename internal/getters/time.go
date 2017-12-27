package getters

import (
	"fmt"
	"math/rand"
)

// RandomTime Getter
type RandomTime struct {
	allowNull bool
}

func (r *RandomTime) Value() interface{} {
	h := rand.Int63n(24)
	m := rand.Int63n(60)
	s := rand.Int63n(60)
	return fmt.Sprintf("'%02d:%02d:%02d'", h, m, s)
}

func (r *RandomTime) String() string {
	return r.Value().(string)
}

func NewRandomTime(allowNull bool) Getter {
	return &RandomTime{allowNull}
}
