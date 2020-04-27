package getters

import (
	"fmt"
	"math/rand"
	"regexp"

	"github.com/brianvoe/gofakeit/v5"
)

// RandomString getter
type RandomString struct {
	name      string
	maxSize   uint64
	allowNull bool
	fn        func() string
}

var (
	emailRe     = regexp.MustCompile(`email`)
	firstNameRe = regexp.MustCompile(`first.*name`)
	lastNameRe  = regexp.MustCompile(`last.*name`)
	nameRe      = regexp.MustCompile(`name`)
	phoneRe     = regexp.MustCompile(`phone`)
	zipRe       = regexp.MustCompile(`zip`)
	colorRe     = regexp.MustCompile(`color`)
	ipAddressRe = regexp.MustCompile(`ip.*(?:address)*`)
	addressRe   = regexp.MustCompile(`address`)
	stateRe     = regexp.MustCompile(`state`)
	cityRe      = regexp.MustCompile(`city`)
	countryRe   = regexp.MustCompile(`country`)
	genderRe    = regexp.MustCompile(`gender`)
	urlRe       = regexp.MustCompile(`url`)
	domainre    = regexp.MustCompile(`domain`)
)

func (r *RandomString) Value() interface{} {
	if r.allowNull && rand.Int63n(100) < nilFrequency {
		return nil
	}

	s := r.fn()
	if len(s) > int(r.maxSize) {
		s = s[:int(r.maxSize)]
	}
	return s
}

func (r *RandomString) String() string {
	v := r.Value()
	if v == nil {
		return NULL
	}
	return v.(string)
}

// Quote returns a quoted string
func (r *RandomString) Quote() string {
	v := r.Value()
	if v == nil {
		return NULL
	}
	return fmt.Sprintf("%q", v)
}

func NewRandomString(name string, maxSize int64, allowNull bool) *RandomString {
	var fn func() string

	switch {
	case emailRe.MatchString(name):
		fn = gofakeit.Email
	case firstNameRe.MatchString(name):
		fn = gofakeit.FirstName
	case lastNameRe.MatchString(name):
		fn = gofakeit.LastName
	case nameRe.MatchString(name):
		fn = gofakeit.Name
	case phoneRe.MatchString(name):
		fn = gofakeit.PhoneFormatted
	case zipRe.MatchString(name):
		fn = gofakeit.Zip
	case colorRe.MatchString(name):
		fn = gofakeit.Color
	case cityRe.MatchString(name):
		fn = gofakeit.City
	case countryRe.MatchString(name):
		fn = gofakeit.Country
	case addressRe.MatchString(name):
		fn = gofakeit.Street
	case ipAddressRe.MatchString(name):
		fn = gofakeit.IPv4Address
	default:
		fn = gofakeit.LoremIpsumWord
	}

	return &RandomString{name, uint64(maxSize), allowNull, fn}
}
