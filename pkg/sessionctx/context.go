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

package sessionctx

import (
	"context"
	"fmt"

	"github.com/secretflow/scql/pkg/sessionctx/variable"
)

// Context is an interface for transaction and executive args environment.
type Context interface {
	GetSessionVars() *variable.SessionVars

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}
}

type BaseContext struct {
	values      map[fmt.Stringer]interface{}
	sessionVars *variable.SessionVars
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewContext creates a new mocked sessionctx.Context.
func NewContext() *BaseContext {
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &BaseContext{
		values:      make(map[fmt.Stringer]interface{}),
		sessionVars: variable.NewSessionVars(),
		ctx:         ctx,
		cancel:      cancel,
	}
	return sctx
}

// GetSessionVars implements the sessionctx.Context GetSessionVars interface.
func (c *BaseContext) GetSessionVars() *variable.SessionVars {
	return c.sessionVars
}

// SetValue implements sessionctx.Context SetValue interface.
func (c *BaseContext) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (c *BaseContext) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}
