// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"fmt"
	"sort"
)

type Metadata struct {
	keys   []string
	values []string
}

func NewMetadata(keys, values []string) Metadata {
	if len(keys) != len(values) {
		panic("arrow: len mismatch")
	}

	n := len(keys)
	if n == 0 {
		return Metadata{}
	}

	md := Metadata{
		keys:   make([]string, n),
		values: make([]string, n),
	}
	copy(md.keys, keys)
	copy(md.values, values)
	return md
}

func MetadataFrom(kv map[string]string) Metadata {
	md := Metadata{
		keys:   make([]string, 0, len(kv)),
		values: make([]string, 0, len(kv)),
	}
	for k := range kv {
		md.keys = append(md.keys, k)
	}
	sort.Strings(md.keys)
	for _, k := range md.keys {
		md.values = append(md.values, kv[k])
	}
	return md
}

func (md Metadata) Len() int         { return len(md.keys) }
func (md Metadata) Keys() []string   { return md.keys }
func (md Metadata) Values() []string { return md.values }

func (kv Metadata) clone() Metadata {
	if len(kv.keys) == 0 {
		return Metadata{}
	}

	o := Metadata{
		keys:   make([]string, len(kv.keys)),
		values: make([]string, len(kv.values)),
	}
	copy(o.keys, kv.keys)
	copy(o.values, kv.values)

	return o
}

// Schema is a sequence of Field values, describing the columns of a table or
// a record batch.
type Schema struct {
	fields []Field
	index  map[string]int
	meta   Metadata
}

// NewSchema returns a new Schema value from the slice of fields and metadata.
//
// NewSchema panics if there are duplicated fields.
// NewSchema panics if there is a field with an invalid DataType.
func NewSchema(fields []Field, metadata *Metadata) *Schema {
	sc := &Schema{
		fields: make([]Field, 0, len(fields)),
		index:  make(map[string]int, len(fields)),
	}
	if metadata != nil {
		sc.meta = metadata.clone()
	}
	for i, field := range fields {
		if field.Type == nil {
			panic("arrow: field with nil DataType")
		}
		sc.fields = append(sc.fields, field)
		if _, dup := sc.index[field.Name]; dup {
			panic(fmt.Errorf("arrow: duplicate field with name %q", field.Name))
		}
		sc.index[field.Name] = i
	}
	return sc
}

func (sc *Schema) Metadata() Metadata { return sc.meta }
func (sc *Schema) Fields() []Field    { return sc.fields }
func (sc *Schema) Field(i int) Field  { return sc.fields[i] }

func (sc *Schema) FieldByName(n string) (Field, bool) {
	i, ok := sc.index[n]
	if !ok {
		return Field{}, ok
	}
	return sc.fields[i], ok
}

// FieldIndex returns the index of the named field or -1.
func (sc *Schema) FieldIndex(n string) int {
	i, ok := sc.index[n]
	if !ok {
		return -1
	}
	return i
}

func (sc *Schema) HasField(n string) bool {
	return sc.FieldIndex(n) >= 0
}

func (sc *Schema) HasMetadata() bool { return len(sc.meta.keys) > 0 }
