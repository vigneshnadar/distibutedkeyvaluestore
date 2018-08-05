package main

import (
"fmt"
"log"
"net/http"
"io/ioutil"
"os"
)

type distributedserver struct {
   Host string `json:"Host"`
   PortNum int `json:"PortNum"`
}

var keyValServer []distributedserver

func handler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hi there, Welcome to distributed key value store")
    fmt.Fprintf(w, "The API for get is /getkv")

    // can add restful routing here
}



func createRequest(apiurl string, reqBody interface{}, apimethod string) *http.Request {
  jsonStr, err := json.Marshal(&reqBody)

  if err != nil {
  fmt.Fprintln(err)
  return nil
  }

  req, e := http.NewRequest(apimethod, apiurl, bytes.NewBuffer(jsonStr))

  if e != nil {
    fmt.Fprintln(err)
    return nil
    }

  return req
}

func getkeyvalue(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf("Hi getting key value")

    allserverreq := make([]*http.Request, 0)

    for i:=0; i < len(distributedserver); i++ {


        getapi := fmt.Sprintf("http://%s:%d/getkv", distributedserver[i].Host, distributedserver[i].PortNum)

        currentReq := createRequest(getapi, nil, http.MethodGet)
        allserverreq = append(allserverreq, currentReq)
    } // end of for

    result, resultcode := requestServers(allserverreq)
    sendResponse(w, r, result, resultcode)
}

func validate(e error) {
if e != nil {
fmt.Fprintf("Error in reading file %v", e)
os.Exit(1)
}
}

func main() {


data, err := ioutil.ReadFile("distributedkvconfig.json")
validate(err)
json.Unmarshal(data, &keyValServer)

http.HandleFunc("/", handler)
http.HandleFunc("/getkv", getkeyvalue)
// http.HandleFunc("/postkv", postkeyvalue)
log.Fatal(http.ListenAndServe(":8080",  nil))
}