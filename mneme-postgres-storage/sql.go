package pgstore

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/fragshq/mneme/errs"
	"github.com/fragshq/mneme/jsonptr"
	"github.com/fragshq/mneme/query"
)

// sqlBuilder accumulates query parameters. Each value becomes a numbered
// placeholder ($n), so the order in which fragments are built never has to
// match their order in the final SQL, and a placeholder can be repeated.
type sqlBuilder struct {
	args []any
	// gin enables containment (@>) pre-filters for the GIN index on data.
	gin bool
}

// arg adds a parameter and returns its placeholder followed by cast (e.g.
// "::jsonb"). json/jsonb parameters must be json.RawMessage.
func (b *sqlBuilder) arg(v any, cast string) string {
	b.args = append(b.args, v)
	return "$" + strconv.Itoa(len(b.args)) + cast
}

// jsonArg adds v, marshaled, as a jsonb parameter.
func (b *sqlBuilder) jsonArg(v any) string {
	raw, _ := json.Marshal(v)
	return b.arg(json.RawMessage(raw), "::jsonb")
}

// literal renders s as an escaped SQL string literal. It is used only where
// parameters cannot go: DDL (index expressions and predicates), and the
// field paths and record type that must read the same in queries so the
// planner can match those indexes.
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// fieldExprs returns, for a JSON-pointer field, its jsonb expression and its
// text expression. Both are SQL NULL when the field is missing. Index DDL
// and queries build them with this one function, so the planner sees the
// same expressions and can match indexes to filters.
func fieldExprs(field string) (string, string, error) {
	p, err := jsonptr.Parse(field)
	if err != nil {
		return "", "", errs.Invalidf("%v", err)
	}
	parts := make([]string, len(p))
	for i, t := range p {
		parts[i] = literal(t)
	}
	path := "ARRAY[" + strings.Join(parts, ",") + "]::text[]"
	return "(data #> " + path + ")", "(data #>> " + path + ")", nil
}

// containmentDoc returns the document {"a":{"b":v}} for the field /a/b, or
// false when the pointer may step into an array (a numeric token or "-"),
// where containment would not mirror the pointer.
func containmentDoc(field string, v any) (any, bool) {
	p, err := jsonptr.Parse(field)
	if err != nil || len(p) == 0 {
		return nil, false
	}
	for _, t := range p {
		if t == "-" || (t != "" && strings.Trim(t, "0123456789") == "") {
			return nil, false
		}
	}
	for i := len(p) - 1; i >= 0; i-- {
		v = map[string]any{p[i]: v}
	}
	return v, true
}

// filter translates f. Under NOT (negated), every leaf is COALESCEd to a
// boolean, so NOT of a missing field is true as the reference semantics
// require. Elsewhere only AND and OR sit above the leaves, and a WHERE
// treats NULL like false, so leaves stay bare and the planner can match
// them to indexes.
func (b *sqlBuilder) filter(f query.Filter, negated bool) (string, error) {
	switch f.Op {
	case query.OpAnd, query.OpOr:
		parts := []string{}
		for _, c := range f.Filters {
			s, err := b.filter(c, negated)
			if err != nil {
				return "", err
			}
			parts = append(parts, s)
		}
		return "(" + strings.Join(parts, " "+strings.ToUpper(string(f.Op))+" ") + ")", nil
	case query.OpNot:
		s, err := b.filter(f.Filters[0], true)
		return "(NOT " + s + ")", err
	}
	s, err := b.leaf(f, negated)
	if err != nil || !negated {
		return s, err
	}
	return "COALESCE(" + s + ", false)", nil
}

var comparison = map[query.Op]string{query.OpEq: "=", query.OpNe: "<>",
	query.OpGt: ">", query.OpGte: ">=", query.OpLt: "<", query.OpLte: "<="}

func (b *sqlBuilder) leaf(f query.Filter, negated bool) (string, error) {
	v := any(nil)
	if f.Op != query.OpExists {
		if err := json.Unmarshal(f.Value, &v); err != nil {
			return "", errs.Invalidf("filter value: %v", err)
		}
	}
	switch f.Field {
	case query.FieldCreatedAt, query.FieldUpdatedAt:
		return b.columnLeaf(timeColumn(f.Field), "::timestamptz", f.Op, v, parseTime)
	case query.FieldID:
		return b.columnLeaf("id", "::text", f.Op, v, asString)
	case query.FieldVersion:
		return b.jsonLeaf(f, v, "to_jsonb(schema_version)", "(schema_version::text)", false)
	case query.FieldLabels:
		// labels is always an array of strings, so a scalar contains is
		// exactly jsonb containment, which the labels GIN index serves.
		if f.Op == query.OpContains && !isComposite(v) {
			return "(labels @> " + b.jsonArg([]any{v}) + ")", nil
		}
		return b.jsonLeaf(f, v, "labels", "(labels #>> '{}')", false)
	}
	j, t, err := fieldExprs(f.Field)
	if err != nil {
		return "", err
	}
	_, containable := containmentDoc(f.Field, nil)
	return b.jsonLeaf(f, v, j, t, b.gin && !negated && containable)
}

func isComposite(v any) bool {
	switch v.(type) {
	case []any, map[string]any:
		return true
	}
	return false
}

// jsonLeaf translates a leaf on a jsonb expression j whose text form is t.
// Each form is exact, and NULL only when the field is missing. With gin, it
// also emits containment pre-filters on data for the GIN index: necessary
// conditions ANDed with the exact test, so they never change the result.
func (b *sqlBuilder) jsonLeaf(f query.Filter, v any, j string, t string, gin bool) (string, error) {
	typ := "jsonb_typeof(" + j + ")"
	contains := func(x any) string {
		doc, _ := containmentDoc(f.Field, x)
		return "data @> " + b.jsonArg(doc)
	}
	switch f.Op {
	case query.OpExists:
		return "(" + j + " IS NOT NULL)", nil
	case query.OpEq:
		s := j + " = " + b.jsonArg(v)
		if gin {
			s = contains(v) + " AND " + s
		}
		return "(" + s + ")", nil
	case query.OpNe:
		return "(" + j + " <> " + b.jsonArg(v) + ")", nil
	case query.OpIn:
		vals, _ := v.([]any)
		s := b.in(j, vals, func(x any) (string, bool) { return b.jsonArg(x), true })
		if gin && len(vals) > 0 {
			ors := []string{}
			for _, x := range vals {
				ors = append(ors, contains(x))
			}
			s = "((" + strings.Join(ors, " OR ") + ") AND " + s + ")"
		}
		return s, nil
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		op := comparison[f.Op]
		switch x := v.(type) {
		case float64:
			c := b.jsonArg(x)
			if f.Op == query.OpGt || f.Op == query.OpGte {
				// jsonb orders null < strings < numbers < booleans, so
				// "above c and below false" is exactly "a number above c",
				// and both bounds can use a jsonb index.
				return "(" + j + " " + op + " " + c + " AND " + j + " < 'false'::jsonb)", nil
			}
			return "(" + j + " " + op + " " + c + " AND " + typ + " = 'number')", nil
		case string:
			return "((" + t + ` COLLATE "C") ` + op + " " + b.arg(x, "::text") + " AND " + typ + " = 'string')", nil
		}
		return "FALSE", nil
	case query.OpPrefix:
		x, ok := v.(string)
		if !ok {
			return "FALSE", nil
		}
		// The lower bound adds nothing logically but lets a byte-order
		// text index serve the prefix.
		p := b.arg(x, "::text")
		return "((" + t + ` COLLATE "C") >= ` + p + " AND starts_with(" + t + ", " + p + ") AND " + typ + " = 'string')", nil
	case query.OpContains:
		elem := ""
		if isComposite(v) {
			elem = "EXISTS (SELECT 1 FROM jsonb_array_elements(" + j + ") AS e(v) WHERE e.v = " + b.jsonArg(v) + ")"
		} else {
			// For a scalar, array containment is exactly element equality.
			elem = j + " @> " + b.jsonArg([]any{v})
		}
		if str, isString := v.(string); isString {
			return "(CASE " + typ + " WHEN 'array' THEN " + elem + " WHEN 'string' THEN strpos(" + t + ", " + b.arg(str, "::text") + ") > 0 ELSE false END)", nil
		}
		s := "CASE WHEN " + typ + " = 'array' THEN " + elem + " ELSE false END"
		if gin {
			// Only a non-string can't match by substring, so only then is
			// containment a necessary condition.
			s = contains([]any{v}) + " AND " + s
		}
		return "(" + s + ")", nil
	}
	return "", errs.Invalidf("unsupported op %s", f.Op)
}

// in returns "(expr IN (...))" over the values conv accepts, each rendered
// by conv, or FALSE when none is accepted.
func (b *sqlBuilder) in(expr string, vals []any, conv func(x any) (string, bool)) string {
	ph := []string{}
	for _, x := range vals {
		if p, ok := conv(x); ok {
			ph = append(ph, p)
		}
	}
	if len(ph) == 0 {
		return "FALSE"
	}
	return "(" + expr + " IN (" + strings.Join(ph, ", ") + "))"
}

// asString and parseTime convert a filter value to a meta column's type:
// $id is text, $createdAt and $updatedAt are timestamps.
func asString(x any) (any, bool) {
	s, ok := x.(string)
	return s, ok
}

func parseTime(x any) (any, bool) {
	s, ok := x.(string)
	if !ok {
		return nil, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	return t, err == nil
}

// columnLeaf translates a leaf on a NOT NULL meta column (id, created_at,
// updated_at), comparing against the column itself so its index stays
// usable. conv converts a value to the column's type, and a value it
// rejects has the other type, which never compares equal or ordered. Only
// text columns support prefix and contains.
func (b *sqlBuilder) columnLeaf(col string, cast string, op query.Op, v any, conv func(x any) (any, bool)) (string, error) {
	cv, ok := conv(v)
	switch op {
	case query.OpExists:
		return "TRUE", nil
	case query.OpEq, query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		if !ok {
			return "FALSE", nil
		}
		return "(" + col + " " + comparison[op] + " " + b.arg(cv, cast) + ")", nil
	case query.OpNe:
		if !ok {
			return "TRUE", nil
		}
		return "(" + col + " <> " + b.arg(cv, cast) + ")", nil
	case query.OpIn:
		vals, _ := v.([]any)
		return b.in(col, vals, func(x any) (string, bool) {
			c, ok := conv(x)
			if !ok {
				return "", false
			}
			return b.arg(c, cast), true
		}), nil
	}
	str, isString := cv.(string)
	if !ok || !isString { // only $id converts to a string
		return "FALSE", nil // prefix and contains need text on both sides
	}
	switch op {
	case query.OpPrefix:
		p := b.arg(str, cast)
		return "(" + col + " >= " + p + " AND starts_with(" + col + ", " + p + "))", nil
	case query.OpContains:
		return "(strpos(" + col + ", " + b.arg(str, cast) + ") > 0)", nil
	}
	return "", errs.Invalidf("unsupported op %s", op)
}

// timeColumn maps a timestamp meta field to its column.
func timeColumn(field string) string {
	if field == query.FieldUpdatedAt {
		return "updated_at"
	}
	return "created_at"
}

// sortKey is one ORDER BY component. A query sort field expands into
// components that reproduce the reference total order (see
// query.CompareValues) without SQL NULLs: type rank, string value (byte
// order), numeric value, raw jsonb. Meta fields are single NOT NULL columns.
type sortKey struct {
	expr  string
	desc  bool
	field int     // index of the query sort field
	part  keyPart // which part of the field's value expr orders by
}

// keyPart says what a sort component holds, so the cursor can supply the
// matching value (see cursorValue).
type keyPart int

const (
	partID      keyPart = iota // the id column
	partVersion                // the schema_version column
	partTime                   // a timestamp column
	partRank                   // a JSON value's type rank
	partStr                    // a JSON string, in byte order ('' otherwise)
	partNum                    // a JSON number (0 otherwise)
	partRaw                    // the jsonb value itself
)

func sortKeys(sorts []query.SortField) ([]sortKey, error) {
	out := []sortKey{}
	for i, s := range sorts {
		j := "labels"
		switch s.Field {
		case query.FieldID:
			out = append(out, sortKey{"id", s.Desc, i, partID})
			continue
		case query.FieldVersion:
			out = append(out, sortKey{"schema_version", s.Desc, i, partVersion})
			continue
		case query.FieldCreatedAt, query.FieldUpdatedAt:
			out = append(out, sortKey{timeColumn(s.Field), s.Desc, i, partTime})
			continue
		case query.FieldLabels:
		default:
			je, _, err := fieldExprs(s.Field)
			if err != nil {
				return nil, err
			}
			j = je
		}
		typ := "jsonb_typeof(" + j + ")"
		out = append(out,
			sortKey{"(CASE WHEN " + j + " IS NULL THEN 6 ELSE CASE " + typ +
				" WHEN 'null' THEN 0 WHEN 'string' THEN 1 WHEN 'number' THEN 2 WHEN 'boolean' THEN 3 WHEN 'array' THEN 4 ELSE 5 END END)", s.Desc, i, partRank},
			sortKey{"(COALESCE(CASE WHEN " + typ + ` = 'string' THEN ` + j + ` #>> '{}' END, '') COLLATE "C")`, s.Desc, i, partStr},
			sortKey{"COALESCE(CASE WHEN " + typ + " = 'number' THEN (" + j + ")::numeric END, 0)", s.Desc, i, partNum},
			sortKey{"COALESCE(" + j + ", 'null'::jsonb)", s.Desc, i, partRaw},
		)
	}
	return out, nil
}

// cursorValue adds the cursor's value for one sort component as a parameter
// and returns its placeholder. Cursors come from clients, so values are
// checked for shape and never inlined.
func (b *sqlBuilder) cursorValue(k sortKey, c query.Cursor) (string, error) {
	bad := errs.Invalidf("malformed cursor")
	ck := c.Keys[k.field]
	v, present := ck.Decoded()
	switch k.part {
	case partID:
		s, ok := v.(string)
		if !ok {
			return "", bad
		}
		return b.arg(s, `::text COLLATE "C"`), nil
	case partVersion:
		n, err := strconv.Atoi(string(ck.Value))
		if err != nil {
			return "", bad
		}
		return b.arg(n, "::integer"), nil
	case partTime:
		s, _ := v.(string)
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return "", bad
		}
		return b.arg(t, "::timestamptz"), nil
	case partRank:
		r := 6
		if present {
			switch v.(type) {
			case nil:
				r = 0
			case string:
				r = 1
			case float64:
				r = 2
			case bool:
				r = 3
			case []any:
				r = 4
			default:
				r = 5
			}
		}
		return b.arg(r, "::integer"), nil
	case partStr:
		s, _ := v.(string)
		return b.arg(s, `::text COLLATE "C"`), nil
	case partNum:
		if _, ok := v.(float64); !ok {
			return "0", nil
		}
		// v decoded as a number, so the raw value is a JSON number literal,
		// which numeric accepts. Sent as text, so the numeric keeps the
		// cursor's exact digits.
		return b.arg(string(ck.Value), "::text::numeric"), nil
	default: // partRaw
		if !present {
			return "'null'::jsonb", nil
		}
		if !json.Valid(ck.Value) {
			return "", bad
		}
		return b.arg(json.RawMessage(ck.Value), "::jsonb"), nil
	}
}

// after builds the keyset condition "row sorts strictly after the cursor".
// The components are never NULL, so when they all share one direction it is
// a row comparison, which a matching btree index (such as (created_at, id)
// for the default sort) serves as a range scan. Mixed directions expand to
// OR over i of (k1 = c1 AND ... AND k(i-1) = c(i-1) AND ki >/< ci), reusing
// each value's placeholder.
func (b *sqlBuilder) after(keys []sortKey, c query.Cursor) (string, error) {
	vals := make([]string, len(keys))
	same := true
	for i, k := range keys {
		v, err := b.cursorValue(k, c)
		if err != nil {
			return "", err
		}
		vals[i] = v
		same = same && k.desc == keys[0].desc
	}
	op := func(k sortKey) string {
		if k.desc {
			return "<"
		}
		return ">"
	}
	if same {
		exprs := []string{}
		for _, k := range keys {
			exprs = append(exprs, k.expr)
		}
		return "((" + strings.Join(exprs, ", ") + ") " + op(keys[0]) + " (" + strings.Join(vals, ", ") + "))", nil
	}
	ors := []string{}
	for i := range keys {
		ands := []string{}
		for j := 0; j < i; j++ {
			ands = append(ands, keys[j].expr+" = "+vals[j])
		}
		ands = append(ands, keys[i].expr+" "+op(keys[i])+" "+vals[i])
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	return "(" + strings.Join(ors, " OR ") + ")", nil
}
