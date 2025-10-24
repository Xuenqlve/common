package errors

import (
	perrors "github.com/pingcap/errors"
)

var (
	Trace     = perrors.Trace
	Cause     = perrors.Cause
	New       = perrors.New
	Errorf    = perrors.Errorf
	Annotate  = perrors.Annotate
	Annotatef = perrors.Annotatef
	Is        = perrors.ErrorEqual
)
