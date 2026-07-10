package config

type Auth struct {
	Secret   string
	Disabled bool
}

func (a Auth) validate() error {
	switch {
	case a.Disabled && a.Secret != "":
		return ErrAuthConflict
	case !a.Disabled && a.Secret == "":
		return ErrAuthNotConfigured
	default:
		return nil
	}
}
