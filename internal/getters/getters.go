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
	oneYear      = 60 * 60 * 24 * 365 * time.Second
)

type Getter interface {
	Value() interface{}
}

type RandomInt struct {
	mask      uint64
	allowNull bool
}

func (r *RandomInt) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	return uint64(rand.Int63n(10e8)) & r.mask
}

func NewRandomInt(mask uint64, allowNull bool) Getter {
	return &RandomInt{mask, allowNull}
}

type RandomIntRange struct {
	min       int64
	max       int64
	allowNull bool
}

func (r *RandomIntRange) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	limit := r.max - r.min + 1
	return r.min + rand.Int63n(limit)
}

func NewRandomIntRange(min, max int64, allowNull bool) Getter {
	return &RandomIntRange{min, max, allowNull}
}

type RandomDecimal struct {
	size      float64
	allowNull bool
}

func (r *RandomDecimal) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	f := rand.Float64() * float64(rand.Int63n(int64(math.Pow10(int(r.size)))))
	format := fmt.Sprintf("%%%0.1ff", r.size)
	return fmt.Sprintf(format, f)
}

func NewRandomDecimal(size float64, allowNull bool) Getter {
	if size == 0 {
		size = 5.2
	}
	return &RandomDecimal{size, allowNull}
}

type RandomString struct {
	maxSize   float64
	allowNull bool
}

func (r *RandomString) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
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

func NewRandomString(maxSize float64, allowNull bool) Getter {
	return &RandomString{maxSize, allowNull}
}

type RandomDate struct {
	allowNull bool
}

func (r *RandomDate) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	d := time.Now().Add(time.Duration(-1*int64(uint64(rand.Int63n(int64(oneYear))))) * time.Second)
	return d.Format("2006-01-02 15:03:04")
}

func NewRandomDate(allowNull bool) Getter {
	return &RandomDate{allowNull}
}

type RandomDateInRange struct {
	min       string
	max       string
	allowNull bool
}

func (r *RandomDateInRange) Value() interface{} {
	d := time.Now().Add(time.Duration(-1*rand.Int63n(int64(oneYear.Seconds()))) * time.Second)
	return d.Format("2006-01-02 15:03:04")
}

func NewRandomDateInRange(min, max string, allowNull bool) Getter {
	if min == "" {
		t := time.Now().Add(-1 * oneYear)
		min = t.Format("2006-01-02")
	}
	return &RandomDateInRange{min, max, allowNull}
}

type RandomDateTimeInRange struct {
	min       string
	max       string
	allowNull bool
}

func (r *RandomDateTimeInRange) Value() interface{} {
	d := time.Now().Add(time.Duration(-1*rand.Int63n(int64(oneYear.Seconds()))) * time.Second)
	return d.Format("2006-01-02 15:03:04")
}

func NewRandomDateTimeInRange(min, max string, allowNull bool) Getter {
	if min == "" {
		t := time.Now().Add(-1 * oneYear)
		min = t.Format("2006-01-02")
	}
	return &RandomDateInRange{min, max, allowNull}
}

func NewRandomDateTime(allowNull bool) Getter {
	return &RandomDateInRange{"", "", allowNull}
}

// RandomTime Getter
type RandomTime struct {
	allowNull bool
}

func (r *RandomTime) Value() interface{} {
	rand.Seed(time.Now().UnixNano())
	h := rand.Int63n(24)
	m := rand.Int63n(60)
	s := rand.Int63n(60)
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
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
	rand.Seed(time.Now().UnixNano())
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}
	i := rand.Int63n(int64(len(r.allowedValues)))
	return r.allowedValues[i]
}

func NewRandomEnum(allowedValues []string, allowNull bool) Getter {
	return &RandomEnum{allowedValues, allowNull}
}

// Constant Getter. Used for debugging
type Constant struct {
}

func (r *Constant) Value() interface{} {
	return "constant"
}

func NewConstant() Getter {
	return &Constant{}
}
