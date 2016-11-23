package qcache

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/kniren/gota/data-frame"
	"net/http"
)

func newDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Header.Get("Content-Type") {
	case "text/csv":
		frame := df.ReadCSV(r.Body)
		if frame.Err() != nil {
			errorMsg := fmt.Sprintf("Could decode CSV data: %v", frame.Err())
			http.Error(w, errorMsg, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case "application/json":
		frame := df.ReadJSON(r.Body)
		if frame.Err() != nil {
			errorMsg := fmt.Sprintf("Could decode JSON data: %v", frame.Err())
			http.Error(w, errorMsg, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "Unknown content type", http.StatusBadRequest)
	}
}

func Application() *mux.Router {
	return Router()
}

func Router() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/qcache/dataset/{key}", newDataset).Methods("POST")
	return r
}
