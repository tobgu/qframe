package qcache_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/gorilla/mux"
	"github.com/tobgu/go-qcache/qcache"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type TestData struct {
	S string
	I int
	F float64
	B bool
}

type QCache struct {
	t   *testing.T
	app *mux.Router
}

func newQCache(t *testing.T) *QCache {
	return &QCache{t: t, app: qcache.Application()}
}

func (c *QCache) insertDataset(key string, headers map[string]string, body io.Reader) *httptest.ResponseRecorder {
	req, err := http.NewRequest("POST", fmt.Sprintf("/qcache/dataset/%s", key), body)
	if err != nil {
		c.t.Fatal(err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)
	return rr
}

func (c *QCache) insertJson(key string, headers map[string]string, input interface{}) {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(input)
	headers["Content-Type"] = "application/json"
	rr := c.insertDataset("FOO", headers, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		c.t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}
}

func (c *QCache) queryDataset(key string, headers map[string]string, q qcache.Query) *httptest.ResponseRecorder {
	jq, err := json.Marshal(q)
	if err != nil {
		c.t.Fatal(err)
	}

	ujq := url.QueryEscape(string(jq[:]))
	req, err := http.NewRequest("GET", fmt.Sprintf("/qcache/dataset/%s?q=%s", key, ujq), nil)
	if err != nil {
		c.t.Fatal(err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)
	return rr
}

func (c *QCache) queryJson(key string, headers map[string]string, q qcache.Query, output interface{}) *httptest.ResponseRecorder {
	headers["Accept"] = "application/json"
	rr := c.queryDataset(key, headers, q)
	if rr.Code != http.StatusOK {
		return rr
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "application/json" {
		c.t.Errorf("Wrong Content-type: %s", contentType)
	}

	err := json.NewDecoder(rr.Body).Decode(output)
	if err != nil {
		c.t.Fatal("Failed to unmarshal JSON")
	}

	return rr
}

func compareTestData(t *testing.T, actual, expected []TestData) {
	if len(actual) == len(expected) {
		if actual[0] != expected[0] {
			t.Errorf("Wrong record content: got %v want %v", actual, expected)
		}
	} else {
		t.Errorf("Wrong record count: got %v want %v", actual, expected)
	}
}

func TestBasicInsertAndQueryCsv(t *testing.T) {
	cache := newQCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	b := new(bytes.Buffer)
	gocsv.Marshal(input, b)
	rr := cache.insertDataset("FOO", map[string]string{"Content-Type": "text/csv"}, b)

	if rr.Code != http.StatusCreated {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}

	rr = cache.queryDataset("FOO", map[string]string{"Accept": "text/csv"}, qcache.Query{})
	if rr.Code != http.StatusOK {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "text/csv" {
		t.Errorf("Wrong Content-type: %s", contentType)
	}

	var output []TestData
	err := gocsv.Unmarshal(rr.Body, &output)
	if err != nil {
		t.Fatal("Failed to unmarshal CSV")
	}

	compareTestData(t, output, input)
}

func TestBasicInsertAndGetJson(t *testing.T) {
	cache := newQCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	output := []TestData{}
	cache.insertJson("FOO", map[string]string{}, input)
	cache.queryJson("FOO", map[string]string{}, qcache.Query{}, &output)
	compareTestData(t, output, input)
}

func TestQueryNonExistingKey(t *testing.T) {
	cache := newQCache(t)
	rr := cache.queryJson("FOO", map[string]string{}, qcache.Query{}, nil)
	if rr.Code != http.StatusNotFound {
		t.Errorf("Unexpected status code: %v", rr.Code)
	}
}

func TestBasicInsertAndQueryWithProjection(t *testing.T) {
	cache := newQCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	output := []TestData{}
	cache.insertJson("FOO", map[string]string{}, input)
	cache.queryJson("FOO", map[string]string{}, qcache.Query{Select: qcache.Clause{"S"}}, &output)
	compareTestData(t, output, []TestData{{S: "Foo"}})
}
