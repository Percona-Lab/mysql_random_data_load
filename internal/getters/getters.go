package getters

const (
	nilFrequency = 10
	oneYear      = int64(60 * 60 * 24 * 365)
)

type Getter interface {
	Value() interface{}
	String() string
}
