package ptdsn

import (
	"fmt"
	"os/user"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/ini.v1"
)

type PTDSN struct {
	Database   string
	Host       string
	Password   string
	Port       int
	Table      string
	User       string
	Protocol   string
	ConfigFile string
}

const (
	defaultMySQLConfigSection = "client"
	defaultConfigFile         = "~/.my.cnf"
)

func (d *PTDSN) String() string {
	return fmt.Sprintf("%v:%v@%v(%v:%v)/%v", d.User, d.Password, d.Protocol, d.Host, d.Port, d.Database)
}

// Parse parses the connection string and returns MySQL connection parameters struct.
func Parse(value string) (*PTDSN, error) {
	d := &PTDSN{}
	parts := strings.Split(value, ",")

	// First, try to parse the values from the config. Those values will be overridden by the other dsn params
	for _, part := range parts {
		m := strings.Split(part, "=")
		key := m[0]
		value := ""
		if len(m) > 1 {
			value = m[1]
		}
		if key == "F" {
			if err := loadMySQLConfigFile(value, d); err != nil {
				return nil, errors.Wrap(err, "cannot parse config file")
			}
			d.ConfigFile = value
		}
	}

	// If there was no F parameter in the dsn, try to load the default ~/.my.cnf
	if d.ConfigFile == "" {
		d.ConfigFile = defaultConfigFile
		// Don't check for error because the config might not exist
		loadMySQLConfigFile(d.ConfigFile, d) // nolint
	}

	for _, part := range parts {
		m := strings.Split(part, "=")
		key := m[0]
		value := ""
		if len(m) > 1 {
			value = m[1]
		}
		switch key {
		case "D":
			d.Database = value
		case "h":
			d.Host = value
			if d.Host == "localhost" {
				d.Protocol = "unix"
			} else {
				d.Protocol = "tcp"
			}
		case "p":
			d.Password = value
		case "P":
			port, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "invalid port")
			}
			d.Port = int(port)
		case "t":
			d.Table = value
		case "u":
			d.User = value
		}
	}

	if d.Protocol == "tcp" && d.Port == 0 {
		d.Port = 3306
	}

	return d, nil
}

func loadMySQLConfigFile(filename string, d *PTDSN) error {
	cfg, err := ini.Load(expandHomeDir(filename))
	if err != nil {
		return err
	}

	section := cfg.Section(defaultMySQLConfigSection)

	if section.HasKey("host") {
		d.Host = section.Key("host").String()
	}

	if section.HasKey("port") {
		portstr := section.Key("port").String()
		port, err := strconv.Atoi(portstr)
		if err != nil {
			return errors.Wrap(err, "invalid port")
		}
		d.Port = port
	}

	if section.HasKey("user") {
		d.User = section.Key("user").String()
	}

	if section.HasKey("password") {
		d.Password = section.Key("password").String()
	}

	return nil
}

func expandHomeDir(dir string) string {
	if !strings.HasPrefix(dir, "~") {
		return dir
	}
	u, err := user.Current()
	if err != nil {
		return dir
	}
	return u.HomeDir + strings.TrimPrefix(dir, "~")
}
