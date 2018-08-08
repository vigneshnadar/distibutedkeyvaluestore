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
"bytes"
"time"
)

type distributedserver struct {
   Host string `json:"Host"`
   PortNum int `json:"PortNum"`
}

type keyValueStore struct {
    Key string `json:"Key"`
    Value *string `json:"Value"`
}

type keyValRequest struct {
    Key string `json:"Key"`
    Value string `json:"Value"`
}

type keyStruct struct {
    Key string `json:"Key"`
}

// an array to store server credentials
var keyValServer []distributedserver

// this function is the default display page for the router
// it displays information about the APIs associated with each request
func handler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hi there, Welcome to distributed key value store")
    fmt.Fprintf(w, "The API for GET is /getallkv")
    fmt.Fprintf(w, "The API for PUT is /putkv")
    fmt.Fprintf(w, "The API for POST is /getkv")
}


// this function takes the api endpoint method type and request body
// creates a new http request from these parameters and returns the request
func createRequest(apiurl string, requestBody interface{}, apimethod string) *http.Request {

// encode the request body
  encodedJson, err := json.Marshal(&requestBody)

  if err != nil {
  fmt.Println(err)
  return nil
  }

  // create a new http request
  newRequest, e := http.NewRequest(apimethod, apiurl, bytes.NewBuffer(encodedJson))

  if e != nil {
    fmt.Println(e)
    return nil
    }

  return newRequest
}


// hash the given string and return an integer
func hash(s string) uint32 {

// this function is used to create a 32 bit hash
h := fnv.New32a()
h.Write([]byte(s))
return h.Sum32()
}

// an array of bytes is decoded into a json array with key value attributes
func loadSetRequest(jsonBytes []byte) []keyValRequest {

var setReqs []keyValRequest
json.Unmarshal(jsonBytes, &setReqs)
return setReqs

}



// an array of bytes is decoded into a json array with key value attributes
func loadServerFetchResp(jsonBytes []byte) []keyValueStore {
var resps []keyValueStore
json.Unmarshal(jsonBytes, &resps)
return resps
}



// this function takes responses from all distributed servers and concatenates all the key values into a single json
func concatenateSetServerResp(resps []*http.Response) ([]byte, int) {


 code := 200

// loop over all response and concatenate json
 for _, response := range resps {

        if response.StatusCode >= 200 {
        code = 200

        } else {
        code = 206
        }
         response.Body.Close()
        } // end of for
         return nil, code
}




// this function takes responses from all distributed servers and concatenates all the key values into a single json
func concatenateFetchServerResp(resps []*http.Response) ([]byte, int) {

 final := make([]keyValueStore, 0)

 code := 200

// loop over all response and concatenate json
 for _, response := range resps {

        if response.StatusCode >= 200 {

        body, err := ioutil.ReadAll(response.Body)

        if err != nil {
        log.Fatal(err)
        }


        // the server returns the keys added
        sresp := loadServerFetchResp(body)



        final = append(final, sresp...)

        } else {
        code = 206
        }
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



// this function takes  array of responses from all distributed servers and concatenates all the key values into a single json array
func concatenateServerResp(resps []*http.Response) ([]byte, int) {

// an array of key value pairs
 final := make([]keyValueStore, 0)

 // success code
 code := 200

// loop over all response and concatenate json
 for _, response := range resps {

        if response.StatusCode >= 200 {

        body, err := ioutil.ReadAll(response.Body)

        if err != nil {
        log.Fatal(err)
        }

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

// it receives an array of http requests
// this function creates a request pool and makes asynchronous GET request to servers
func requestServers(reqs []*http.Request) ([]byte, int) {

     // when a server is requested it should be locked so that write does not occur at same time
     var mutex = &sync.Mutex{}

     // a wait group is created . a wait group allows for asynchronous processing of tasks
     // the processor will wait untill all the tasks in the wait group are completed
     var wg sync.WaitGroup


     // create an array of http requests
     resps := make([]*http.Response, 0)

    // add the count of request to wait group
     wg.Add(len(reqs))

     // iterate through all the requests
     for _, curReq := range reqs {

     // used for creating a concurrent pipeline
     go func(curReq *http.Request)   {

     // the done function decreases the count of wait group by one
     defer wg.Done()

     // the request is sent to the server
     curReq.Header.Set("Content-type", "application/json")
     client := &http.Client{}
     resp, err := client.Do(curReq)
     if err == nil {

       // the response is appended to an array of responses
       mutex.Lock()
       resps = append(resps, resp)
       mutex.Unlock()
       }

     }(curReq)
     } //end of for
     // the processor waits till all the requests are processed
     wg.Wait()

     // the array of responses is sent so that the key value pairs from all those responses
     // can be combined into a single json array
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
     if err == nil {

       mutex.Lock()
       resps = append(resps, resp)
       mutex.Unlock()
       }

     }(curReq)
     } //end of for

     wg.Wait()

     return concatenateSetServerResp(resps)


}


// this function creates a request pool and makes asynchronous POST request to servers
func requestFetchServers(reqs []*http.Request) ([]byte, int) {

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
     if err == nil {

       mutex.Lock()
       resps = append(resps, resp)
       mutex.Unlock()
       }

     }(curReq)
     } //end of for

     wg.Wait()

     return concatenateFetchServerResp(resps)


}


// sends the response of all concat json value
func sendResponse(w http.ResponseWriter, r *http.Request, reply []byte, code int) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(code)
w.Write(reply)
}

// this function returns all the key values stored in all the servers
func getkeyvalue(w http.ResponseWriter, r *http.Request) {

    // start the timer to measure performance of the GET request
    start := time.Now()

    // create an array of type http request
    allserverreq := make([]*http.Request, 0)

    // iterate over all the servers and make a get request one by one
    for i:=0; i < len(keyValServer); i++ {

        // create an url to hit the endpoint of the server
        getapi := fmt.Sprintf("http://%s:%d/fetch", keyValServer[i].Host, keyValServer[i].PortNum)

        // pass the api and the request type so that a request can be created
        currentReq := createRequest(getapi, nil, http.MethodGet)

        // an array of requests is created
        allserverreq = append(allserverreq, currentReq)
    } // end of for

    // pass the array of requests so that the actual requests can be made
    // the response and the status code of the response is returned
    result, resultcode := requestServers(allserverreq)

    // write the response to the writer and send it to the client
    sendResponse(w, r, result, resultcode)

    // once the response is sent back. stop the timer and display it to track metrics
    fmt.Println("TOTAL TIME TAKEN FOR PUT REQUEST: %s", time.Since(start))
}


// check if there is an error in reading file
func validate(e error) {
if e != nil {
// fmt.Fprintf("Error in reading file %v", e)
os.Exit(1)
}
}


// Hnadles the PUT REQUEST. Puts a key value in the store
func putkeyvalue(w http.ResponseWriter, r *http.Request) {
    start := time.Now()
    var meth = "PUT"
    fmt.Printf("Hi putting key value %s\n", meth)

    body, err := ioutil.ReadAll(r.Body)

    if err != nil {
    log.Fatal(err)
    }


    setReqs := loadSetRequest(body)


    serverReqMap := make(map[int] []keyValRequest)

    for i:=0; i < len(setReqs); i++ {


    keyVal := setReqs[i].Key

    fmt.Printf("the key is %s\n", keyVal)
    fmt.Printf("the value is %s\n", setReqs[i].Value)
    keyValStr := keyVal



    // select server by mod logic
    serverIdx := int(hash(keyValStr)) % len(keyValServer)

    fmt.Printf("Server Id %d\n", serverIdx)

    // at the same time there can be multiple requests so append them
    serverReqMap[serverIdx] = append(serverReqMap[serverIdx], setReqs[i])

    } // end of for



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

        fmt.Println("TOTAL TIME TAKEN FOR GET REQUEST: %s", time.Since(start))
    } // end of func


// receives an array of bytes. decodes it and puts it in a json array with attribute key
func extractKey(jsonBytes []byte) []keyStruct {
var req []keyStruct
json.Unmarshal(jsonBytes, &req)
return req
}

// Handles the POST REQUEST. A key is posted and a value is returned
func getvalue(w http.ResponseWriter, r *http.Request) {
    start :=  time.Now()
    var meth = "POST"
    fmt.Printf("Hi GET corresponding  value %s\n", meth)


    body, err := ioutil.ReadAll(r.Body)

    if err != nil {
    log.Fatal(err)
    }

    keyReceived := extractKey(body)



    serverReqMap := make(map[int] []keyStruct)

    for i:=0; i < len(keyReceived); i++ {

    // keyEncoding := setReqs[i].Key.Encoding
    keyVal := keyReceived[i].Key
    keyValStr := keyVal




    // select server by hash mod logic
    serverIdx := int(hash(keyValStr)) % len(keyValServer)

    fmt.Printf("Server Id %d\n", serverIdx)

    // at the same time there can be multiple requests so append them
    serverReqMap[serverIdx] = append(serverReqMap[serverIdx], keyReceived[i])

    } // end of for



   // array of requests
    reqs := make([]*http.Request, 0)

    for i:=0; i < len(keyValServer); i++ {

            serverReqs :=  serverReqMap[i]

            // check if there is a request present
            fmt.Printf("Hi putting key value %d\n", len(serverReqs))
            if len(serverReqs) > 0 {
            fmt.Println("server request\n")
            serverEndPoint := fmt.Sprintf("http://%s:%d/getkv", keyValServer[i].Host, keyValServer[i].PortNum)


             httpReq := createRequest(serverEndPoint, serverReqs, http.MethodPost)
                    reqs = append(reqs, httpReq)
                    } // end of if
                } // end of for

                result, resultcode := requestFetchServers(reqs)
                sendResponse(w, r, result, resultcode)

        fmt.Println("TOTAL TIME TAKEN FOR POST REQUEST: %s", time.Since(start))
    } // end of func

func main() {

// reads all the servers IP and port from config file
data, err := ioutil.ReadFile("distributedkvconfig.json")

// check if there were errors while reading from file
validate(err)

// stores the array of server credentials
json.Unmarshal(data, &keyValServer)

// routing handlers specified here
http.HandleFunc("/", handler)

// gets all the key values
http.HandleFunc("/getallkv", getkeyvalue)

// a key is posted and a value is returned
http.HandleFunc("/getkv", getvalue)

// puts a key value in store
http.HandleFunc("/putkv", putkeyvalue)

// start the server on port 8080
log.Fatal(http.ListenAndServe(":8080",  nil))
}
