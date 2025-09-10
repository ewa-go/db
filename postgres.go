package postgres

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ewa-go/crud"
)

type Postgres struct{}

const (
	inArray = "&& ARRAY[?]"
)

func (p *Postgres) Pattern() string {
	return `\[(->|->>|>|<|>-|<-|!|<>|array|&&|!array|!&&|~|!~|~\*|!~\*|\+|!\+|%|:|[aA-zZ]+)]$`
}

func (p *Postgres) Format(r *crud.CRUD, q *crud.QueryParam) (*crud.QueryParam, error) {

	switch q.Znak {
	case "!":
		q.Znak = "!="
	case ">-":
		q.Znak = ">="
	case "<-":
		q.Znak = "<="
	case "%":
		q.Znak = "like"
		q.Type = "::text"
	case "!%":
		q.Znak = "not like"
	case "~", "!~", "~*", "!~*":
	case "+":
		q.Znak = "similar to"
	case "!+":
		//q.Type += "::text"
		q.Znak = "not similar to"
	case "->", "->>":
		switch v := q.Value.(type) {
		case string:
			a := strings.Split(v, "=")
			if len(a) == 2 {
				qf := crud.QueryFormat(r, a[0], a[1])
				value := fmt.Sprintf("'%s' %s", qf.Key, qf.Znak)
				switch t := qf.Value.(type) {
				case string:
					value = strings.ReplaceAll(value, "?", "'"+t+"'")
				case []string:
					for i, tt := range t {
						value = strings.Replace(value, "?", "'"+tt+"'", i+1)
					}
				}
				q.Value = value
			}
		}
	case "array", "&&":
		if q.IsArray() {
			q.Znak = inArray
			return p.setTypeArray(q), nil
		}
	case "!array", "!&&":
		if q.IsArray() {
			q.Znak = inArray
			q.Key = fmt.Sprintf(`not "%s"`, q.Key)
			q.IsQuotes = false
			return p.setTypeArray(q), nil
		}
	}

	if q.Value == nil {
		switch q.Znak {
		case "=":
			q.Znak = "is null"
		case "!=":
			q.Znak = "is not null"
		}
		return q, nil
	}
	if q.IsArray() {
		switch q.Znak {
		case "=":
			q.Znak = "in(?)"
		case "!=", "<>":
			q.Znak = "not in(?)"
		}
		return q, nil
	}
	if q.IsRange() {
		q.Znak = "between ? and ?"
		return q, nil
	}

	q.Znak += " ?"

	return q, nil
}

func (p *Postgres) setTypeArray(q *crud.QueryParam) *crud.QueryParam {
	if q.Znak == inArray {
		var (
			array    string
			dataType string
		)
		switch q.DataType {
		case "string":
			dataType = "::text[]"
			if v, ok := q.Value.([]string); ok && len(v) > 0 {
				array = "'" + strings.Join(v, "','") + "'"
			}
		case "int":
			dataType = "::int[]"
			if v, ok := q.Value.([]int); ok && len(v) > 0 {
				for i, val := range v {
					array += strconv.Itoa(val)
					if i < len(v)-1 {
						array += ","
					}
				}
			}
		case "int64":
			dataType = "::bigint[]"
			if v, ok := q.Value.([]int64); ok && len(v) > 0 {
				for i, val := range v {
					array += fmt.Sprintf("%d", val)
					if i < len(v)-1 {
						array += ","
					}
				}
			}
		case "float":
			dataType = "::real[]"
			if v, ok := q.Value.([]float32); ok && len(v) > 0 {
				for i, val := range v {
					array += fmt.Sprintf("%f", val)
					if i < len(v)-1 {
						array += ","
					}
				}
			}
		case "float64":
			dataType = "::double precision[]"
			if v, ok := q.Value.([]float64); ok && len(v) > 0 {
				for i, val := range v {
					array += fmt.Sprintf("%f", val)
					if i < len(v)-1 {
						array += ","
					}
				}
			}
		case "uint":
			dataType = "::serial[]"
			if v, ok := q.Value.([]uint); ok && len(v) > 0 {
				for i, val := range v {
					array += fmt.Sprintf("%d", val)
					if i < len(v)-1 {
						array += ","
					}
				}
			}
		case "uint64":
			dataType = "::bigserial[]"
			if v, ok := q.Value.([]uint64); ok && len(v) > 0 {
				for i, val := range v {
					array += fmt.Sprintf("%d", val)
					if i < len(v)-1 {
						array += ","
					}
				}
			}
		}

		q.Znak = strings.ReplaceAll(q.Znak, "?", array) + dataType
		q.Value = nil
	}
	return q
}

func (p *Postgres) Query(q *crud.QueryParams, columns []string) (query string, values []any) {
	var (
		params []*crud.QueryParam
		fields []string
	)
	// Если нет параметров, то выходим
	if q.Len() == 0 && q.ID == nil {
		return "", nil
	}
	// Отдельно передаём поле ID
	if q.ID != nil {
		params = append(params, q.ID)
	}
	// Заполнение параметры адресной строки
	for key, value := range q.Get() {
		if key == crud.AllFieldsParamName || key == crud.ExtraParamName {
			continue
		}
		for _, v := range value {
			params = append(params, v)
		}
	}

	// Формирование полей для поиска везде OR
	if vals, ok := q.Get()[crud.AllFieldsParamName]; ok && len(vals) > 0 {
		value := vals[0]
		// Параметр адресной строки *=
		if q.Filter != nil && len(q.Filter.Fields) > 0 {
			for _, field := range q.Filter.Fields {
				if _, ok = q.Get()[field]; !ok {
					value.Key = field
					if value.IsQuotes {
						value.Key = `"` + value.Key + `"::text`
					}
					fields = append(fields, strings.Trim(fmt.Sprintf("%s %s", value.Key, value.Znak), " "))
					values = append(values, value.Value)
				}
			}
		} else {
			for _, column := range columns {
				if _, ok = q.Get()[column]; !ok {
					value.Key = column
					if value.IsQuotes {
						value.Key = `"` + value.Key + `"::text`
					}
					fields = append(fields, strings.Trim(fmt.Sprintf("%s %s", value.Key, value.Znak), " "))
					values = append(values, value.Value)
				}
			}
		}
	}
	if len(fields) > 0 {
		for i, field := range fields {
			var spliter string
			if i < len(fields)-1 {
				spliter = " or "
			}
			query += field + spliter
		}
		query = "(" + query + ")"
	}
	// Заполняем строку запроса и значения для неё
	if len(params) > 0 {
		var v string
		for i, param := range params {
			values = append(values, param.Value)
			var spliter string
			if i > 0 {
				spliter = " and "
			}
			if param.IsOR {
				spliter = " or "
			}
			if param.IsQuotes {
				param.Key = `"` + param.Key + `"`
			}
			if param.Znak == "like ?" {
				param.Key += string(param.Type)
			}
			v += spliter + strings.Trim(fmt.Sprintf("%s %s", param.Key, param.Znak), " ")
		}
		if len(query) > 0 {
			query += " and " + v
		} else {
			query = v
		}
	}

	// Избавляемся от nil значений, так как gorm будет ломаться
	var vals []any
	for _, value := range values {
		if value != nil {
			vals = append(vals, value)
		}
	}

	return query, vals
}

// Cast Приведение переменной к типу данных
func (p *Postgres) Cast(value string, q *crud.QueryParam) (err error) {

	if q.DataType == "" {
		switch strings.ToLower(value) {
		case "null":
			q.Value = nil
		case "true", "false":
			q.Value, err = strconv.ParseBool(value)
		default:
			if rng, ok := p.IsRange(q.Znak, value); ok {
				q.Value = rng
				q.Type = crud.RangeType
				break
			}
			if array, ok := p.IsArray(value); ok {
				q.Value = array
				q.Type = crud.ArrayType
				break
			}
			q.Value = value
		}
		return
	}
	var (
		rng, array []string
		ok         bool
	)
	if rng, ok = p.IsRange(q.Znak, value); ok {
		q.Type = crud.RangeType
	}
	if array, ok = p.IsArray(value); ok && !q.IsRange() {
		q.Type = crud.ArrayType
	}
	switch q.DataType {
	case "string":
		switch {
		case q.IsArray():
			q.Value = array
		case q.IsRange():
			q.Value = rng
		default:
			q.Value = value
		}
	case "int":
		switch {
		case q.IsArray():
			q.Value = p.SetInt32Array(array)
		case q.IsRange():
			q.Value = p.SetInt32Array(rng)
		default:
			q.Value, err = strconv.Atoi(value)
		}
	case "int64":
		switch {
		case q.IsArray():
			q.Value = p.SetInt64Array(array)
		case q.IsRange():
			q.Value = p.SetInt64Array(rng)
		default:
			q.Value, err = strconv.ParseInt(value, 10, 64)
		}
	case "float":
		switch {
		case q.IsArray():
			q.Value = p.SetFloat32Array(array)
		case q.IsRange():
			q.Value = p.SetFloat32Array(rng)
		default:
			q.Value, err = strconv.ParseFloat(value, 32)
		}
	case "float64":
		switch {
		case q.IsArray():
			q.Value = p.SetFloat64Array(array)
		case q.IsRange():
			q.Value = p.SetFloat64Array(rng)
		default:
			q.Value, err = strconv.ParseFloat(value, 64)
		}
	case "uint":
		switch {
		case q.IsArray():
			q.Value = p.SetUIntArray(array)
		case q.IsRange():
			q.Value = p.SetUIntArray(rng)
		default:
			q.Value, err = strconv.ParseUint(value, 10, 32)
		}
	case "uint64":
		switch {
		case q.IsArray():
			q.Value = p.SetUInt64Array(array)
		case q.IsRange():
			q.Value = p.SetUInt64Array(rng)
		default:
			q.Value, err = strconv.ParseUint(value, 10, 64)
		}
	case "date":
		if q.IsRange() {
			q.Value = p.SetTimeArray(rng, time.DateOnly)
			break
		}
		q.Value, err = time.Parse(time.DateOnly, value)
	case "time":
		if q.IsRange() {
			q.Value = p.SetTimeArray(rng, time.TimeOnly)
			break
		}
		q.Value, err = time.Parse(time.TimeOnly, value)
	case "datetime":
		if q.IsRange() {
			q.Value = p.SetTimeArray(rng, time.DateTime)
			break
		}
		q.Value, err = time.Parse(time.DateTime, value)
	default:

	}
	if err != nil {
		return fmt.Errorf("invalid datatype %s", q.DataType)
	}
	return nil
}

// IsArray Проверка на массив
func (*Postgres) IsArray(value string) ([]string, bool) {
	rgx := regexp.MustCompile(`^\[(.+)]$`)
	if rgx.MatchString(value) {
		matches := rgx.FindStringSubmatch(value)
		if len(matches) == 2 {
			return strings.Split(matches[1], ","), true
		}
	}
	return nil, false
}

func (*Postgres) IsRange(znak, value string) ([]string, bool) {
	if znak == ":" {
		rgx := regexp.MustCompile(`^\[(.+)\|(.+)]$`)
		if rgx.MatchString(value) {
			matches := rgx.FindStringSubmatch(value)
			if len(matches) == 3 {
				return []string{matches[1], matches[2]}, true
			}
		}
	}
	return nil, false
}

func (*Postgres) SetInt32Array(array []string) (a []int) {
	for _, v := range array {
		if value, err := strconv.Atoi(v); err == nil {
			a = append(a, value)
		}
	}
	return a
}

func (*Postgres) SetInt64Array(array []string) (a []int64) {
	for _, v := range array {
		if value, err := strconv.ParseInt(v, 10, 64); err == nil {
			a = append(a, value)
		}
	}
	return a
}

func (*Postgres) SetUIntArray(array []string) (a []uint64) {
	for _, v := range array {
		if value, err := strconv.ParseUint(v, 10, 32); err == nil {
			a = append(a, value)
		}
	}
	return a
}

func (*Postgres) SetUInt64Array(array []string) (a []uint64) {
	for _, v := range array {
		if value, err := strconv.ParseUint(v, 10, 64); err == nil {
			a = append(a, value)
		}
	}
	return a
}

func (*Postgres) SetFloat32Array(array []string) (a []float32) {
	for _, v := range array {
		if value, err := strconv.ParseFloat(v, 32); err == nil {
			a = append(a, float32(value))
		}
	}
	return a
}

func (*Postgres) SetFloat64Array(array []string) (a []float64) {
	for _, v := range array {
		if value, err := strconv.ParseFloat(v, 64); err == nil {
			a = append(a, value)
		}
	}
	return a
}

func (*Postgres) SetTimeArray(array []string, layout string) (a []time.Time) {
	for _, v := range array {
		if value, err := time.Parse(layout, v); err == nil {
			a = append(a, value)
		}
	}
	return a
}
