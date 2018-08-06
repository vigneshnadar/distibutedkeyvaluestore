package main

import (
"fmt"
"log"
"net/http"
"io/ioutil"
"hash/fnv"
"os"
"sync"
"encoding/json"
"encoding/base64"
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


type serverSetResp struct {
    KeysAdded int `json:"keys_added"`
    KeysFailed []Encoded `json:"keys_failed"`
}

type clientSetReq struct {
    Key Encoded `json:"key"`
    Value Encoded `json:"value"`
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

// read the body and return. body is returned as byte array
func loadReqBody(resp *http.Request) []byte {
body, readErr := ioutil.ReadAll(resp.Body)

if readErr != nil {
log.Fatal(readErr)
return nil
}

return body
}


func binToStr(s string) (string, bool) {
  realStr := base64.StdEncoding.EncodeToString([]byte(s))
  return realStr, true
}



func hash(s string) uint32 {
h := fnv.New32a()
h.Write([]byte(s))
return h.Sum32()
}

// read the body and return. body is returned as byte array
func loadSetRequest(jsonBytes []byte) []clientSetReq {

var setReqs []clientSetReq
json.Unmarshal(jsonBytes, &setReqs)
return setReqs

}



// takes the body as bytes , unmarshalls it and returns the struct
func loadServerSetResp(jsonBytes []byte) serverSetResp {
var resps serverSetResp
json.Unmarshal(jsonBytes, &resps)
return resps
}

// takes the body as bytes , unmarshalls it and returns the struct
func loadServerFetchResp(jsonBytes []byte) []serverFetchResp {
var resps []serverFetchResp
json.Unmarshal(jsonBytes, &resps)
return resps
}



// this function takes responses from all distributed servers and concatenates all the key values into a single json
func concatenateSetServerResp(resps []*http.Response) ([]byte, int) {

 keysFailed := make([]Encoded, 0)
 keysAdded := 0
 code := 200

// loop over all response and concatenate json
 for _, response := range resps {

        if response.StatusCode >= 200 {
        body := loadRespBody(response)


        // the server returns the keys added
        sresp := loadServerSetResp(body)

        keysAdded += sresp.KeysAdded

        keysFailed = append(keysFailed, sresp.KeysFailed...)

        } else {
        code = 206
        }
         response.Body.Close()
        } // end of for

        final := serverSetResp{KeysAdded: keysAdded, KeysFailed: keysFailed}
        // the json body is encoded
        body, err := json.Marshal(final)

        if err != nil {
            // fmt.Fprintln(err)
            return nil, 500
            }

        return body, code

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

// this function creates a request pool and makes asynchronous GET request to servers
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


// this function creates a request pool and makes asynchronous PUT request to servers
func requestSetServers(reqs []*http.Request) ([]byte, int) {

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

     return concatenateSetServerResp(resps)


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


        getapi := fmt.Sprintf("http://%s:%d/fetch", keyValServer[i].Host, keyValServer[i].PortNum)

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


// Hnadles the PUT REQUEST
func putkeyvalue(w http.ResponseWriter, r *http.Request) {
    var meth = "PUT"
    fmt.Printf("Hi putting key value %s\n", meth)


    body := loadReqBody(r)
    setReqs := loadSetRequest(body)


    isValid := true
    serverReqMap := make(map[int] []clientSetReq)

    for i:=0; i < len(setReqs); i++ {

    // keyEncoding := setReqs[i].Key.Encoding
    keyVal := setReqs[i].Key.Data
    keyValStr := keyVal

    // if keyEncoding == "binary" {
      //  keyValStr, isValid := binToStr(keyValStr)
    // }

    if !isValid {
    break
    }

    // select server by mod logic
    serverIdx := int(hash(keyValStr)) % len(keyValServer)

    fmt.Printf("Server Id %d\n", serverIdx)

    // at the same time there can be multiple requests so append them
    serverReqMap[serverIdx] = append(serverReqMap[serverIdx], setReqs[i])

    } // end of for


    if !isValid {
    return
    }

   // array of requests
    reqs := make([]*http.Request, 0)

    for i:=0; i < len(keyValServer); i++ {

            serverReqs :=  serverReqMap[i]

            // check if there is a request present
            fmt.Printf("Hi putting key value %d\n", len(serverReqs))
            if len(serverReqs) > 0 {
            fmt.Println("server request\n")
            serverEndPoint := fmt.Sprintf("http://%s:%d/set", keyValServer[i].Host, keyValServer[i].PortNum)


             httpReq := createRequest(serverEndPoint, serverReqs, http.MethodPut)
                    reqs = append(reqs, httpReq)
                    } // end of if
                } // end of for

                result, resultcode := requestSetServers(reqs)
                sendResponse(w, r, result, resultcode)


    } // end of func


func main() {

// reads all the servers IP and port from config file
data, err := ioutil.ReadFile("distributedkvconfig.json")
validate(err)
json.Unmarshal(data, &keyValServer)

   // routing
http.HandleFunc("/", handler)
http.HandleFunc("/fetch", getkeyvalue)
http.HandleFunc("/putkv", putkeyvalue)
log.Fatal(http.ListenAndServe(":8080",  nil))
}
