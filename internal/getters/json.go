package getters

import (
	"fmt"
	"math/rand"

	"encoding/json"

	"github.com/icrowley/fake"
)

// RandomJson getter
type RandomJson struct {
	name      string
	allowNull bool
}

func (r *RandomJson) Value() interface{} {
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}

	return map[string]string{
		fake.Word(): fake.Sentence(),
	}
}

func (r *RandomJson) String() string {
	m := r.Value()
	if m == nil {
		return NULL
	}
	s, _ := json.Marshal(m)
	return string(s)
}

func (r *RandomJson) Quote() string {
	m := r.Value()
	d, _ := json.Marshal(m)
	s := string(d)
	return fmt.Sprintf("%q", s)
}

func NewRandomJson(name string, allowNull bool) *RandomJson {
	return &RandomJson{name, allowNull}
}
