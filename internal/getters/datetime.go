package getters

import (
	"fmt"
	"math/rand"
	"time"
)

type RandomDateTimeInRange struct {
	min       string
	max       string
	allowNull bool
}

func (r *RandomDateTimeInRange) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	randomSeconds := rand.Int63n(oneYear)
	d := time.Now().Add(-1 * time.Duration(randomSeconds) * time.Second)
	return d
}

func (r *RandomDateTimeInRange) String() string {
	d := r.Value().(time.Time)
	return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:03:04"))
}

func NewRandomDateTimeInRange(name string, min, max string, allowNull bool) Getter {
	if min == "" {
		t := time.Now().Add(-1 * time.Duration(oneYear) * time.Second)
		min = t.Format("2006-01-02")
	}
	return &RandomDateInRange{name, min, max, allowNull}
}

func NewRandomDateTime(name string, allowNull bool) Getter {
	return &RandomDateInRange{name, "", "", allowNull}
}
