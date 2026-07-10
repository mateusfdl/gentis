package config

import "fmt"

type TLS struct {
	Cert string
	Key  string
}

func (t TLS) validate(section string) error {
	if (t.Cert == "") != (t.Key == "") {
		return fmt.Errorf("%w (%s)", ErrTLSIncomplete, section)
	}

	return nil
}
