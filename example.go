package fivetran

import (
	// required for Google Cloud Functions:
	// _ "github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"encoding/json"
	fconn "github.com/oviva-ag/fivetran/connector"
	"log"
	"net/http"
)

//connector is the singleton responsible to process Fivetran requests and fetch data from the source
var connector fconn.Connector

func init() {

	// TODO `tableConnector` implements the `fconn.TableConnector` interface and connects to the source
	// this may also be a list of multiple tables, depending on the source data
	var tableConnector fconn.TableConnector

	newConnector, err := fconn.NewConnector([]fconn.TableConnector{tableConnector})
	if err != nil {
		log.Fatalf("failed to create connector: %s", err)
	}

	connector = newConnector
}

//Handler is the handler exposed for Google Cloud Functions
func Handler(w http.ResponseWriter, r *http.Request) {

	var req fconn.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("json.NewDecoder: %v", err)
		http.Error(w, "error parsing request", http.StatusBadRequest)
		return
	}

	res, err := connector.Sync(r.Context(), &req)
	if err != nil {
		log.Printf("failed to sync connector: %s", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("failed to marshal response: %s", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
