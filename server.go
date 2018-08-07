package main

import(
  "os"
  "fmt"
  "log"
  "net/http"
  "encoding/json"
  "io/ioutil"
)

// Data structure for handling requests
// and also used as the default response structure
// for fetch/get.
type request_data struct{
  Key string
  Value string
}

// Data structure for responding to queries.
// Value is true if key exists, false otherwise.
type query_response struct{
  Key string
  Value bool
}

// This is the Key-Value Store associated with this server instance.
var keyvalue_store map[string]string = make(map[string]string)

// Retrieves all key-value pairs and posts them back.
func handleGetAllKeys(w http.ResponseWriter, r *http.Request){
fmt.Println("getall")
  m := make([]request_data, len(keyvalue_store))
  i := 0
  for k, v := range keyvalue_store{
    m[i] = request_data{Key: k, Value: v}
    i += 1
  }
  output, err := json.Marshal(m)
	if err != nil{
		panic(err)
	}

  w.Header().Set("Content-Type","application/json")
  w.Write(output)
}

func handleFetch(w http.ResponseWriter, r *http.Request){
    // Retrieves key-value pairs for requested keys.
    keys := make([]request_data,0)
    err := json.NewDecoder(r.Body).Decode(&keys)
  	if err != nil{
  		panic(err)
  	}

    m := make([]request_data,len(keys))
    for index, element := range keys{
      _, exists := keyvalue_store[element.Key]
      if !exists{
        // Value is set to NULL if the key does not exist in the server.
        m[index] = request_data{Key: element.Key, Value: "NULL"}
      } else{
        m[index] = request_data{Key: element.Key, Value: keyvalue_store[element.Key]}
      }
    }

  	output, err := json.Marshal(m)
  	if err != nil{
  		panic(err)
  	}

    w.Header().Set("Content-Type","application/json")
    w.Write(output)
}

// Inserts/Updates the key-value pair.
func handleSet(w http.ResponseWriter, r *http.Request){
  keys := make([]request_data,0)
  fmt.Println("inside set")

  body, readErr := ioutil.ReadAll(r.Body)

  if readErr != nil {
  log.Fatal(readErr)
  }

  json.Unmarshal(body, &keys)
  //err := json.NewDecoder(r.Body).Decode(&keys)


	// if err != nil{
	//	panic(err)
	// }
fmt.Println("before for")
  for _, element := range keys{
    keyvalue_store[element.Key] = element.Value
    fmt.Printf("inside for %d", element.Key)
  }
  fmt.Println("after for")

  fmt.Fprintf(w, "SET COMPLETE\n")
}

// Handles queries about the existence of provided keys.
func handleQuery(w http.ResponseWriter, r *http.Request){
  keys := make([]request_data,0)
  err := json.NewDecoder(r.Body).Decode(&keys)
	if err != nil{
		panic(err)
	}

  m := make([]query_response,len(keys))
  for index, element := range keys {
    _, exists := keyvalue_store[element.Key]
    m[index] = query_response{Key: element.Key, Value: exists}
  }

  output, err := json.Marshal(m)
  if err != nil {
    http.Error(w,err.Error(), 500)
    return
  }
  w.Header().Set("content-type","application/json")
  w.Write(output)
}


func main(){
  // Default port.
  serverPort := ":8080"

  // go run main.go portNumber
  if len(os.Args) == 2{

    serverPort = ":" + os.Args[1]
    fmt.Printf("here %s", serverPort)
  } else if len(os.Args) > 2{
    log.Println("Invalid number of arguments. Proceeding with default port 8080.\n")
  }

	// muxr := mux.NewRouter()

 //  muxr.HandleFunc("/query", handleQuery).Methods("POST")
  // muxr.HandleFunc("/getkv", handleFetch).Methods("POST")
  http.HandleFunc("/fetch", handleGetAllKeys)
   http.HandleFunc("/set", handleSet)

  log.Fatal(http.ListenAndServe(serverPort, nil))
}