package qcache_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/tobgu/go-qcache/qcache"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type TestInput struct {
	S string
	I int
	F float64
	B bool
}

func insertDataset(t *testing.T, key string, headers map[string]string, body io.Reader) *httptest.ResponseRecorder {
	req, err := http.NewRequest("POST", fmt.Sprintf("/qcache/dataset/%s", key), body)
	if err != nil {
		t.Fatal(err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	qcache.Router().ServeHTTP(rr, req)
	return rr
}

func TestBasicInsertAndQueryJson(t *testing.T) {
	input := []TestInput{{S: "Foo", I: 123, F: 1.5, B: true}}
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(input)
	rr := insertDataset(t, "FOO", map[string]string{"Content-Type": "application/json"}, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}

	//// Check the response body is what we expect.
	//expected := `{"alive": true}`
	//if rr.Body.String() != expected {
	//	t.Errorf("handler returned unexpected body: got %v want %v",
	//		rr.Body.String(), expected)
	//}
}

func TestBasicInsertAndQueryCsv(t *testing.T) {
	input := []TestInput{{S: "Foo", I: 123, F: 1.5, B: true}}
	b := new(bytes.Buffer)
	gocsv.Marshal(input, b)
	rr := insertDataset(t, "FOO", map[string]string{"Content-Type": "text/csv"}, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}

	//// Check the response body is what we expect.
	//expected := `{"alive": true}`
	//if rr.Body.String() != expected {
	//	t.Errorf("handler returned unexpected body: got %v want %v",
	//		rr.Body.String(), expected)
	//}
}
