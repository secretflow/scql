// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/terror"
)

var (
	ErrUnknown                         = terror.ClassOptimizer.New(mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
	ErrUnknownColumn                   = terror.ClassOptimizer.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	ErrAmbiguous                       = terror.ClassOptimizer.New(mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])
	ErrUnsupportedType                 = terror.ClassOptimizer.New(mysql.ErrUnsupportedType, mysql.MySQLErrName[mysql.ErrUnsupportedType])
	ErrDupFieldName                    = terror.ClassOptimizer.New(mysql.ErrDupFieldName, mysql.MySQLErrName[mysql.ErrDupFieldName])
	ErrInvalidWildCard                 = terror.ClassOptimizer.New(mysql.ErrInvalidWildCard, mysql.MySQLErrName[mysql.ErrInvalidWildCard])
	ErrNoDB                            = terror.ClassOptimizer.New(mysql.ErrNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
	ErrBadTable                        = terror.ClassOptimizer.New(mysql.ErrBadTable, mysql.MySQLErrName[mysql.ErrBadTable])
	ErrNonUniqTable                    = terror.ClassOptimizer.New(mysql.ErrNonuniqTable, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	ErrIllegalReference                = terror.ClassOptimizer.New(mysql.ErrIllegalReference, mysql.MySQLErrName[mysql.ErrIllegalReference])
	ErrWrongGroupField                 = terror.ClassOptimizer.New(mysql.ErrWrongGroupField, mysql.MySQLErrName[mysql.ErrWrongGroupField])
	ErrInvalidGroupFuncUse             = terror.ClassOptimizer.New(mysql.ErrInvalidGroupFuncUse, mysql.MySQLErrName[mysql.ErrInvalidGroupFuncUse])
	ErrWrongUsage                      = terror.ClassOptimizer.New(mysql.ErrWrongUsage, mysql.MySQLErrName[mysql.ErrWrongUsage])
	ErrWindowInvalidWindowFuncAliasUse = terror.ClassOptimizer.New(mysql.ErrWindowInvalidWindowFuncAliasUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncAliasUse])
	ErrWrongArguments                  = terror.ClassOptimizer.New(mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	ErrTablenameNotAllowedHere         = terror.ClassOptimizer.New(mysql.ErrTablenameNotAllowedHere, mysql.MySQLErrName[mysql.ErrTablenameNotAllowedHere])
	ErrWrongNumberOfColumnsInSelect    = terror.ClassOptimizer.New(mysql.ErrWrongNumberOfColumnsInSelect, mysql.MySQLErrName[mysql.ErrWrongNumberOfColumnsInSelect])
	ErrDBaccessDenied                  = terror.ClassOptimizer.New(mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied               = terror.ClassOptimizer.New(mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
	ErrPrivilegeCheckFail              = terror.ClassOptimizer.New(mysql.ErrPrivilegeCheckFail, mysql.MySQLErrName[mysql.ErrPrivilegeCheckFail])
	ErrSpecificAccessDenied            = terror.ClassOptimizer.New(mysql.ErrSpecificAccessDenied, mysql.MySQLErrName[mysql.ErrSpecificAccessDenied])
	ErrViewInvalid                     = terror.ClassOptimizer.New(mysql.ErrViewInvalid, mysql.MySQLErrName[mysql.ErrViewInvalid])
	ErrWindowInvalidWindowFuncUse      = terror.ClassOptimizer.New(mysql.ErrWindowInvalidWindowFuncUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncUse])
	ErrWindowNoSuchWindow              = terror.ClassOptimizer.New(mysql.ErrWindowNoSuchWindow, mysql.MySQLErrName[mysql.ErrWindowNoSuchWindow])
	ErrWindowCircularityInWindowGraph  = terror.ClassOptimizer.New(mysql.ErrWindowCircularityInWindowGraph, mysql.MySQLErrName[mysql.ErrWindowCircularityInWindowGraph])
	ErrWindowNoChildPartitioning       = terror.ClassOptimizer.New(mysql.ErrWindowNoChildPartitioning, mysql.MySQLErrName[mysql.ErrWindowNoChildPartitioning])
	ErrWindowNoInherentFrame           = terror.ClassOptimizer.New(mysql.ErrWindowNoInherentFrame, mysql.MySQLErrName[mysql.ErrWindowNoInherentFrame])
	ErrWindowNoRedefineOrderBy         = terror.ClassOptimizer.New(mysql.ErrWindowNoRedefineOrderBy, mysql.MySQLErrName[mysql.ErrWindowNoRedefineOrderBy])
	ErrWindowDuplicateName             = terror.ClassOptimizer.New(mysql.ErrWindowDuplicateName, mysql.MySQLErrName[mysql.ErrWindowDuplicateName])
	ErrPartitionClauseOnNonpartitioned = terror.ClassOptimizer.New(mysql.ErrPartitionClauseOnNonpartitioned, mysql.MySQLErrName[mysql.ErrPartitionClauseOnNonpartitioned])
	ErrWindowFrameStartIllegal         = terror.ClassOptimizer.New(mysql.ErrWindowFrameStartIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameStartIllegal])
	ErrWindowFrameEndIllegal           = terror.ClassOptimizer.New(mysql.ErrWindowFrameEndIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameEndIllegal])
	ErrWindowFrameIllegal              = terror.ClassOptimizer.New(mysql.ErrWindowFrameIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameIllegal])
	ErrWindowRangeFrameOrderType       = terror.ClassOptimizer.New(mysql.ErrWindowRangeFrameOrderType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameOrderType])
	ErrWindowRangeFrameTemporalType    = terror.ClassOptimizer.New(mysql.ErrWindowRangeFrameTemporalType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameTemporalType])
	ErrWindowRangeFrameNumericType     = terror.ClassOptimizer.New(mysql.ErrWindowRangeFrameNumericType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameNumericType])
	ErrWindowRangeBoundNotConstant     = terror.ClassOptimizer.New(mysql.ErrWindowRangeBoundNotConstant, mysql.MySQLErrName[mysql.ErrWindowRangeBoundNotConstant])
	ErrWindowRowsIntervalUse           = terror.ClassOptimizer.New(mysql.ErrWindowRowsIntervalUse, mysql.MySQLErrName[mysql.ErrWindowRowsIntervalUse])
	ErrWindowFunctionIgnoresFrame      = terror.ClassOptimizer.New(mysql.ErrWindowFunctionIgnoresFrame, mysql.MySQLErrName[mysql.ErrWindowFunctionIgnoresFrame])
	ErrNotSupportedYet                 = terror.ClassOptimizer.New(mysql.ErrNotSupportedYet, mysql.MySQLErrName[mysql.ErrNotSupportedYet])
)
