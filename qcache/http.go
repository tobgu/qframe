package qcache

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/kniren/gota/dataframe"
	"log"
	"net/http"
)

type application struct {
	cache Cache
}

func (a *application) newDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	key := vars["key"]

	switch r.Header.Get("Content-Type") {
	case "text/csv":
		frame := dataframe.ReadCSV(r.Body)
		if frame.Err != nil {
			errorMsg := fmt.Sprintf("Could decode CSV data: %v", frame.Err)
			http.Error(w, errorMsg, http.StatusBadRequest)
			return
		}
		a.cache.Put(key, &QFrame{dataFrame: &frame})
		w.WriteHeader(http.StatusCreated)
	case "application/json":
		frame := dataframe.ReadJSON(r.Body)
		if frame.Err != nil {
			errorMsg := fmt.Sprintf("Could decode JSON data: %v", frame.Err)
			http.Error(w, errorMsg, http.StatusBadRequest)
			return
		}
		a.cache.Put(key, &QFrame{dataFrame: &frame})
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "Unknown content type", http.StatusBadRequest)
	}
}

func (a *application) queryDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	frame := a.cache.Get(key)
	if frame == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var err error = nil
	r.ParseForm()
	if qstring := r.Form.Get("q"); qstring != "" {
		frame, err = frame.Query(qstring)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusBadRequest)
		}
	}

	accept := r.Header.Get("Accept")
	w.Header().Set("Content-Type", accept)

	switch accept {
	case "text/csv":
		err = frame.dataFrame.WriteCSV(w)
	case "application/json":
		err = frame.dataFrame.WriteJSON(w)
	default:
		http.Error(w, "Unknown accept type", http.StatusBadRequest)
		return
	}

	if err != nil {
		log.Fatalf("Failed writing JSON: %v", err)
	}
}

func Application() *mux.Router {
	app := application{cache: newMapCache()}
	r := mux.NewRouter()
	r.HandleFunc("/qcache/dataset/{key}", app.newDataset).Methods("POST")
	r.HandleFunc("/qcache/dataset/{key}", app.queryDataset).Methods("GET")
	return r
}
