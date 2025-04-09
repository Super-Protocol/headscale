package types

import "gorm.io/gorm"

type HeadscaleNode struct {
	gorm.Model

	GlobalID string

	Host string

	Port uint16
}
