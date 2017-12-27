package getters

import "fmt"

// Constant Getter. Used for debugging
type Constant struct {
	value interface{}
}

func (r *Constant) Value() interface{} {
	return r.value
}

func (r *Constant) String() string {
	return fmt.Sprintf("%q", r.Value())
}

func NewConstant(value interface{}) Getter {
	return &Constant{value}
}
