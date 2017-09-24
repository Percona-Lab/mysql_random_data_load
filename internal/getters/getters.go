package getters

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/icrowley/fake"
)

const (
	nilFrequency = 10
	oneYear      = int64(60 * 60 * 24 * 365)
)

type Getter interface {
	Value() interface{}
	String() string
}

type RandomInt struct {
	name      string
	mask      int64
	allowNull bool
}

func (r *RandomInt) Value() interface{} {
	return rand.Int63n(r.mask)
}

func (r *RandomInt) String() string {
	return fmt.Sprintf("%d", r.Value())
}

func NewRandomInt(name string, mask int64, allowNull bool) Getter {
	return &RandomInt{name, mask, allowNull}
}

type RandomIntRange struct {
	name      string
	min       int64
	max       int64
	allowNull bool
}

func (r *RandomIntRange) Value() interface{} {
	limit := r.max - r.min + 1
	return r.min + rand.Int63n(limit)
}

func (r *RandomIntRange) String() string {
	return fmt.Sprintf("%d", r.Value())
}

func NewRandomIntRange(name string, min, max int64, allowNull bool) Getter {
	return &RandomIntRange{name, min, max, allowNull}
}

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

// RandomString getter
type RandomString struct {
	name      string
	maxSize   int64
	allowNull bool
}

func (r *RandomString) Value() interface{} {
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}
	var s string
	maxSize := uint64(r.maxSize)
	if maxSize == 0 {
		maxSize = uint64(rand.Int63n(100))
	}

	if maxSize <= 10 {
		s = fake.FirstName()
	} else if maxSize < 30 {
		s = fake.FullName()
	} else {
		s = fake.Sentence()
	}
	if len(s) > int(maxSize) {
		s = s[:int(maxSize)]
	}
	return s
}

func (r *RandomString) String() string {
	return fmt.Sprintf("%q", r.Value())
}

func NewRandomString(name string, maxSize int64, allowNull bool) Getter {
	return &RandomString{name, maxSize, allowNull}
}

type RandomDate struct {
	name      string
	allowNull bool
}

func (r *RandomDate) Value() interface{} {
	var randomSeconds time.Duration
	for i := 0; i < 10 && randomSeconds != 0; i++ {
		randomSeconds = time.Duration(rand.Int63n(int64(oneYear)) + rand.Int63n(100))
	}
	d := time.Now().Add(-1 * randomSeconds)
	return d
}

func (r *RandomDate) String() string {
	d := r.Value().(time.Time)
	return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:03:04"))
}

func NewRandomDate(name string, allowNull bool) Getter {
	return &RandomDate{name, allowNull}
}

type RandomDateInRange struct {
	name      string
	min       string
	max       string
	allowNull bool
}

func (r *RandomDateInRange) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	var randomSeconds int64
	randomSeconds = rand.Int63n(oneYear) + rand.Int63n(100)
	d := time.Now().Add(-1 * time.Duration(randomSeconds) * time.Second)
	return d
}

func (r *RandomDateInRange) String() string {
	d := r.Value().(time.Time)
	return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:03:04"))
}

func NewRandomDateInRange(name string, min, max string, allowNull bool) Getter {
	if min == "" {
		t := time.Now().Add(-1 * time.Duration(oneYear) * time.Second)
		min = t.Format("2006-01-02")
	}
	return &RandomDateInRange{name, min, max, allowNull}
}

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

// RandomTime Getter
type RandomTime struct {
	allowNull bool
}

func (r *RandomTime) Value() interface{} {
	h := rand.Int63n(24)
	m := rand.Int63n(60)
	s := rand.Int63n(60)
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func (r *RandomTime) String() string {
	return r.Value().(string)
}

func NewRandomTime(allowNull bool) Getter {
	return &RandomTime{allowNull}
}

// RandomEnum Getter
type RandomEnum struct {
	allowedValues []string
	allowNull     bool
}

func (r *RandomEnum) Value() interface{} {
	//rand.Seed(time.Now().UnixNano())
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}
	i := rand.Int63n(int64(len(r.allowedValues)))
	return r.allowedValues[i]
}

func (r *RandomEnum) String() string {
	return fmt.Sprintf("%q", r.Value())
}

func NewRandomEnum(allowedValues []string, allowNull bool) Getter {
	return &RandomEnum{allowedValues, allowNull}
}

type RandomSample struct {
	name      string
	samples   []interface{}
	allowNull bool
}

func (r *RandomSample) Value() interface{} {
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}
	pos := rand.Int63n(int64(len(r.samples)))
	return r.samples[pos]
}

func (r *RandomSample) String() string {
	switch r.Value().(type) {
	case string:
		return fmt.Sprintf("%q", r.Value())
	default:
		return fmt.Sprintf("%v", r.Value())
	}
}

func NewRandomSample(name string, samples []interface{}, allowNull bool) *RandomSample {
	r := &RandomSample{name, samples, allowNull}
	return r
}

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
