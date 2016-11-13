package qcache

import (
	"net/http"
	"github.com/gorilla/mux"
	"log"
	"github.com/kniren/gota/data-frame"
	"bytes"
)


func NewDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	contentType := r.Header.Get("Content-Type")
	if contentType == "text/csv" {
		// TODO: Get rid of this, ReadCSV should take a Reader
		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		s := buf.String()
		frame := df.ReadCSV(s)
		csvData, err := frame.SaveCSV()

		if err != nil {
			http.Error(w, "Could not convert data to CSV", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/csv")
		_, err = w.Write(csvData)
		if err != nil {
			// Is there anything reasonable that could be done here?
			log.Fatal(err)
		}
	}
}


func Serve() {
	r := mux.NewRouter()
	r.HandleFunc("/qcache/dataset/{category}", NewDataset).Methods("POST")
	log.Fatal(http.ListenAndServe(":8888", r))
}
