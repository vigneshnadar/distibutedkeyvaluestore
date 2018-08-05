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


func getkeyvalue(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf("Hi getting key value")

    // can add restful rputing here
}

func validate(e error) {
if(e != nil) {
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