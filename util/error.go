package util

import (
	"os"

	"github.com/allspace/csmgr/common"
)

func ErrorCode(code int) error {
	if code == 0 {
		return nil
	}
	switch code {
	case fscommon.EEXIST:
		return os.ErrExist
	case fscommon.ENOENT:
		return os.ErrNotExist
	case fscommon.EINVAL:
		return os.ErrInvalid
	case fscommon.EPERM:
		return os.ErrPermission
	}
	return &CSError{code: code}
}

func ErrorMsg(msg string) *CSError {
	return &CSError{msg: msg, code: -1}
}

type CSError struct {
	code int
	msg  string
}

func (me *CSError) Error() string {
	switch me.code {
	case fscommon.EIO:
		return "IO error."
	}

	return "Unkown error."
}
