package postgres

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ewa-go/crud"
	"github.com/ewa-go/ewa"
)

func assertEq(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("Values did not match, a: %v, b: %v\n", a, b)
	}
}

func assertArrayEq(t *testing.T, a []interface{}, b []interface{}) {
	if len(a) != len(b) {
		t.Fatalf("Values did not match, a: %v, b: %v\n", a, b)
	}
	for i, aa := range a {
		switch at := aa.(type) {
		case []string, []int, []time.Time:
			assertArrayStringEq(t, at, b[i])
		default:
			assertEq(t, aa, b[i])
		}
	}
}

func assertArrayStringEq(t *testing.T, a interface{}, b interface{}) {
	if fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b) {
		t.Fatalf("Values did not match, a: %v, b: %v\n", a, b)
	}
}

type Functions struct{}

func (f Functions) Columns(r *crud.CRUD, fields ...string) []string {
	return []string{"id", "name"}
}

func (f Functions) SetRecord(c *ewa.Context, r *crud.CRUD, data *crud.Body, params *crud.QueryParams) (int, any, error) {
	return 200, 0, nil
}

func (f Functions) GetRecord(c *ewa.Context, r *crud.CRUD, params *crud.QueryParams) (int, crud.Map, error) {
	return 200, nil, nil
}

func (f Functions) GetRecords(c *ewa.Context, r *crud.CRUD, params *crud.QueryParams) (int, crud.Maps, int64, error) {
	return 200, nil, 0, nil
}

func (f Functions) UpdateRecord(c *ewa.Context, r *crud.CRUD, data *crud.Body, params *crud.QueryParams) (int, any, error) {
	return 200, nil, nil
}

func (f Functions) DeleteRecord(c *ewa.Context, r *crud.CRUD, params *crud.QueryParams) (int, any, error) {
	return 200, nil, nil
}

func (f Functions) Audit(action string, c *ewa.Context, r *crud.CRUD) {
	fmt.Println(action)
	if c.Identity != nil {
		fmt.Println(c.Identity.Username)
	}
	fmt.Println(r.ModelName)
}

func (f Functions) Unmarshal(body *crud.Body, contentType string, data []byte) (err error) {
	switch contentType {
	case "application/json", "application/json;utf-8":
		if body.IsArray {
			return json.Unmarshal(data, &body.Array)
		}
		return json.Unmarshal(data, &body.Data)
	case "application/xml":
		if body.IsArray {
			return xml.Unmarshal(data, &body.Array)
		}
		return xml.Unmarshal(data, &body.Data)
	}
	return nil
}

func getCRUD() *crud.CRUD {
	return crud.New(new(Functions))
}

func getArrayQueryParam(key, dataType string) *crud.QueryParam {
	return &crud.QueryParam{
		Key:      strings.Trim(key, " "),
		Znak:     "&&",
		Type:     crud.ArrayType,
		DataType: dataType,
	}
}

func TestPattern(t *testing.T) {
	pg := Postgres{}
	assertEq(t, pg.Pattern(), `\[(->|->>|>|<|>-|<-|!|<>|array|&&|!array|!&&|~|!~|~\*|!~\*|\+|!\+|%|:|[aA-zZ]+)]$`)
}

func TestCastFormatArrayString(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1,2]"
	q := getArrayQueryParam("ids", "string")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY['1','2']::text[]")
}

func TestCastFormatArrayInt(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1,2]"
	q := getArrayQueryParam("ids", "int")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY[1,2]::int[]")
}

func TestCastFormatArrayInt64(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1,2]"
	q := getArrayQueryParam("ids", "int64")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY[1,2]::bigint[]")
}

func TestCastFormatArrayFloat(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1.1,2.2]"
	q := getArrayQueryParam("ids", "float")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY[1.100000,2.200000]::real[]")
}

func TestCastFormatArrayFloat64(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1.1,2.2]"
	q := getArrayQueryParam("ids", "float64")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY[1.100000,2.200000]::double precision[]")
}

func TestCastFormatArrayUint(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1,2]"
	q := getArrayQueryParam("ids", "uint")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY[1,2]::serial[]")
}

func TestCastFormatArrayUint64(t *testing.T) {
	pg := Postgres{}
	r := getCRUD()
	value := "[1,2]"
	q := getArrayQueryParam("ids", "uint64")
	if err := pg.Cast(value, q); err != nil {
		t.Fatal(err)
	}
	param, err := pg.Format(r, q)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, param.Znak, "&& ARRAY[1,2]::bigserial[]")
}
