package qcache

import (
	"net/http"
	"github.com/gorilla/mux"
	"log"
	"github.com/kniren/gota/data-frame"
)


func NewDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	contentType := r.Header.Get("Content-Type")
	if contentType == "text/csv" {
		frame := df.ReadCSV(r.Body)
		err := frame.WriteCSV(w)

		if err != nil {
			http.Error(w, "Could not convert data to CSV", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/csv")
	}
}


func Serve() {
	r := mux.NewRouter()
	r.HandleFunc("/qcache/dataset/{category}", NewDataset).Methods("POST")
	log.Fatal(http.ListenAndServe(":8888", r))
}
