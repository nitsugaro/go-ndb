package ndb

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	goutils "github.com/nitsugaro/go-utils"
)

const (
	sepCond = "/"
	sepAnd  = ";"
	sepNot  = "not" + sepCond
)

func NewQueryFromURIParams(schema string, method string, params map[string][]string) (*Query, error) {
	var q *Query

	switch method {
	case "GET":
		q = NewReadQuery(schema)
	case "POST":
		q = NewCreateQuery(schema)
	case "PUT":
		q = NewUpdateQuery(schema)
	case "DELETE":
		q = NewDeleteQuery(schema)
	default:
		return nil, fmt.Errorf("invalid query method")
	}

	if v := first(params, "f"); v != "" {
		q.Fields(splitCSV(v)...)
	}

	if len(params["q"]) != 0 && method != "POST" {
		q.Where(goutils.Map(params["q"], func(q string, _ int) M {
			return parseExpr(strings.TrimSpace(q))
		})...)
	}

	if v := first(params, "l"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Limit(n)
		}
	}

	if v := first(params, "o"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Offset(n)
		}
	}

	if len(params["j"]) != 0 && method == "GET" {
		for _, raw := range params["j"] {
			applyJoin(q, raw)
		}
	}

	return q, nil
}

func applyJoin(q *Query, raw string) {
	s := unesc(raw)
	parts := strings.SplitN(s, sepCond, 3)
	if len(parts) < 2 {
		return
	}

	table := parts[0]
	typ := parts[1]

	var expr string
	if len(parts) == 3 {
		expr = parts[2]
	}

	j := q.NewJoin(table, JoinType(typ))

	if strings.TrimSpace(expr) != "" {
		j.On(parseExpr(expr))
	}

	j.DoneJoin()
}

func parseExpr(raw string) M {
	s := unesc(raw)
	group := M{}
	andParts := strings.Split(s, sepAnd)

	for _, a := range andParts {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}

		if strings.HasPrefix(a, sepNot) {
			m := parseCond(strings.TrimPrefix(a, sepNot))
			n, ok := group["not"].(M)
			if !ok {
				n = M{}
				group["not"] = n
			}
			for k, v := range m {
				n[k] = v
			}
			continue
		}

		m := parseCond(a)
		for k, v := range m {
			group[k] = v
		}
	}

	return group
}

func parseCond(s string) M {
	p := strings.SplitN(s, sepCond, 3)
	if len(p) != 3 {
		return M{}
	}

	field := p[0]
	op := p[1]
	val := p[2]

	if op == "eq" {
		return M{field: parseVal(val)}
	}

	if op == "eqf" {
		return M{field: M{"eq_field": val}}
	}

	if op == "isnull" {
		b, _ := strconv.ParseBool(val)
		return M{field: M{"isnull": b}}
	}

	if op == "in" || op == "notin" {
		val = strings.TrimPrefix(val, "(")
		val = strings.TrimSuffix(val, ")")
		items := splitCSV(val)
		arr := make([]any, 0, len(items))
		for _, it := range items {
			arr = append(arr, parseVal(it))
		}
		return M{field: M{op: arr}}
	}

	return M{field: M{op: parseVal(val)}}
}

func parseVal(s string) any {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") && len(s) >= 2 {
		return s[1 : len(s)-1]
	}

	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}

	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}

	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	return s
}

func first(params map[string][]string, key string) string {
	v := params[key]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func unesc(s string) string {
	u, err := url.QueryUnescape(s)
	if err != nil {
		return s
	}
	return u
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
