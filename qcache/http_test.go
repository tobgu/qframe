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

func (c *QCache) queryDataset(key string, headers map[string]string) *httptest.ResponseRecorder {
	req, err := http.NewRequest("GET", fmt.Sprintf("/qcache/dataset/%s", key), nil)
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

func TestBasicInsertAndQueryJson(t *testing.T) {
	qcache := newQCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(input)
	rr := qcache.insertDataset("FOO", map[string]string{"Content-Type": "application/json"}, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}

	rr = qcache.queryDataset("FOO", map[string]string{"Accept": "application/json"})
	if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Wrong Content-type: %s", contentType)
	}

	var output []TestData
	err := json.NewDecoder(rr.Body).Decode(&output)
	if err != nil {
		t.Fatal("Failed to unmarshal JSON")
	}

	if len(output) == len(input) {
		if output[0] != input[0] {
			t.Errorf("Wrong record content: got %v want %v", output, input)
		}
	} else {
		t.Errorf("Wrong record count: got %v want %v", output, input)
	}
}

func TestBasicInsertAndQueryCsv(t *testing.T) {
	qcache := newQCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	b := new(bytes.Buffer)
	gocsv.Marshal(input, b)
	rr := qcache.insertDataset("FOO", map[string]string{"Content-Type": "text/csv"}, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}

	rr = qcache.queryDataset("FOO", map[string]string{"Accept": "text/csv"})
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

	if len(output) == len(input) {
		if output[0] != input[0] {
			t.Errorf("Wrong record content: got %v want %v", output, input)
		}
	} else {
		t.Errorf("Wrong record count: got %v want %v", output, input)
	}
}
