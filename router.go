package main

import (
"fmt"
"log"
"net/http"
"io/ioutil"
"os"
"sync"
"encoding/json"
"bytes"
)

type distributedserver struct {
   Host string `json:"Host"`
   PortNum int `json:"PortNum"`
}

type Encoded struct {
 Encoding string `json:"encoding"`
 Data string `json:"data"`
}


type serverFetchResp struct {
    Key Encoded `json:"key"`
    Value *Encoded `json:"value"`
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
  // fmt.Fprintln(err)
  return nil
  }

  req, e := http.NewRequest(apimethod, apiurl, bytes.NewBuffer(jsonStr))

  if e != nil {
    // fmt.Fprintln(err)
    return nil
    }

  return req
}


// read the body and return. body is returned as byte array
func loadRespBody(resp *http.Response) []byte {
body, readErr := ioutil.ReadAll(resp.Body)

if readErr != nil {
log.Fatal(readErr)
}

return body
}

// takes the body as bytes , unmarshalls it and returns the struct
func loadServerFetchResp(jsonBytes []byte) []serverFetchResp {
var resps []serverFetchResp
json.Unmarshal(jsonBytes, &resps)
return resps
}



// this function takes responses from all distributed servers and concatenates all the key values into a single json
func concatenateServerResp(resps []*http.Response) ([]byte, int) {

 final := make([]serverFetchResp, 0)
 code := 200

// loop over all response and concatenate json
 for _, response := range resps {

        if response.StatusCode >= 200 {
        body := loadRespBody(response)
        sresp := loadServerFetchResp(body)

        final = append(final, sresp...)

        } // end of if
         response.Body.Close()
        } // end of for

        // the json body is encoded
        body, err := json.Marshal(final)

        if err != nil {
            // fmt.Fprintln(err)
            return nil, 500
            }

        return body, code

}

func requestServers(reqs []*http.Request) ([]byte, int) {

     // when a server is requested it should be locked so that write does not occur at same time
     var mutex = &sync.Mutex{}
     var wg sync.WaitGroup

     resps := make([]*http.Response, 0)

     wg.Add(len(reqs))

     for _, curReq := range reqs {

     go func(curReq *http.Request)   {
     defer wg.Done()
     curReq.Header.Set("Content-type", "application/json")
     client := &http.Client{}
     resp, err := client.Do(curReq)
     if err != nil {
       panic(err)
       } else {
       mutex.Lock()
       resps = append(resps, resp)
       mutex.Unlock()
       }

     }(curReq)
     } //end of for

     wg.Wait()

     return concatenateServerResp(resps)


}


// sends the response of all concat json value
func sendResponse(w http.ResponseWriter, r *http.Request, reply []byte, code int) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(code)
w.Write(reply)
}

func getkeyvalue(w http.ResponseWriter, r *http.Request) {
    // fmt.Fprintln("Hi getting key value")

    allserverreq := make([]*http.Request, 0)

    for i:=0; i < len(keyValServer); i++ {


        getapi := fmt.Sprintf("http://%s:%d/getkv", keyValServer[i].Host, keyValServer[i].PortNum)

        currentReq := createRequest(getapi, nil, http.MethodGet)
        allserverreq = append(allserverreq, currentReq)
    } // end of for

    result, resultcode := requestServers(allserverreq)
    sendResponse(w, r, result, resultcode)
}

func validate(e error) {
if e != nil {
// fmt.Fprintf("Error in reading file %v", e)
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