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






// this function takes responses from all distributed servers and concatenates all the key values into a single json
func concatenateSetServerResp(severResponse []*http.Response) ([]byte, int) {

 code := 200

// loop over all response and concatenate json
 for _, response := range severResponse {

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
func concatenateFetchServerResp(severResponse []*http.Response) ([]byte, int) {

 arrKV := make([]keyValueStore, 0)

 code := 200

// loop over all response and concatenate json
 for _, response := range severResponse {

        if response.StatusCode >= 200 {

        body, err := ioutil.ReadAll(response.Body)

        if err != nil {
        log.Fatal(err)
        }


        // the server returns the keys added

        var responseKV []keyValueStore
        json.Unmarshal(body, &responseKV)





        arrKV = append(arrKV, responseKV...)

        } else {
        code = 206
        }
         response.Body.Close()
        } // end of for


        // the json body is encoded
        body, err := json.Marshal(arrKV)

        if err != nil {
            fmt.Println(err)
            return nil, 500
            }

        return body, code

}



// this function takes  array of responses from all distributed servers and concatenates all the key values into a single json array
func concatenateServerResp(severResponse []*http.Response) ([]byte, int) {

// an array of key value pairs
 arrKV := make([]keyValueStore, 0)

 // success code
 code := 200

// loop over all response and concatenate json
 for _, response := range severResponse {

        if response.StatusCode >= 200 {

        body, err := ioutil.ReadAll(response.Body)

        if err != nil {
        log.Fatal(err)
        }


        var responseKV []keyValueStore
        json.Unmarshal(body, &responseKV)


        arrKV = append(arrKV, responseKV...)

        } // end of if
         response.Body.Close()
        } // end of for

        // the json body is encoded
        body, err := json.Marshal(arrKV)

        if err != nil {
            fmt.Println(err)
            return nil, 500
            }

        return body, code

}

// it receives an array of http requests
// this function creates a request pool and makes asynchronous GET request to servers
func requestServers(arrReq []*http.Request) ([]byte, int) {

     // when a server is requested it should be locked so that write does not occur at same time
     var serverSync = &sync.Mutex{}

     // a wait group is created . a wait group allows for asynchronous processing of tasks
     // the processor will wait untill all the tasks in the wait group are completed
     var serverWaitGroup sync.WaitGroup


     // create an array of http requests
     severResponse := make([]*http.Response, 0)

    // add the count of request to wait group
     serverWaitGroup.Add(len(arrReq))

     // iterate through all the requests
     for _, currentRequest := range arrReq {

     // used for creating a concurrent pipeline
     go func(currentRequest *http.Request)   {

     // the done function decreases the count of wait group by one
     defer serverWaitGroup.Done()

     // the request is sent to the server
     currentRequest.Header.Set("Content-type", "application/json")
     requestShooter := &http.Client{}
     resp, err := requestShooter.Do(currentRequest)
     if err == nil {

       // the response is appended to an array of responses
       serverSync.Lock()
       severResponse = append(severResponse, resp)
       serverSync.Unlock()
       }

     }(currentRequest)
     } //end of for
     // the processor waits till all the requests are processed
     serverWaitGroup.Wait()

     // the array of responses is sent so that the key value pairs from all those responses
     // can be combined into a single json array
     return concatenateServerResp(severResponse)


}


// this function creates a request pool and makes asynchronous PUT request to servers
func requestSetServers(arrReq []*http.Request) ([]byte, int) {

     // when a server is requested it should be locked so that write does not occur at same time
     var serverSync = &sync.Mutex{}
     var serverWaitGroup sync.WaitGroup

     severResponse := make([]*http.Response, 0)

     serverWaitGroup.Add(len(arrReq))

     for _, currentRequest := range arrReq {

     go func(currentRequest *http.Request)   {
     defer serverWaitGroup.Done()
     currentRequest.Header.Set("Content-type", "application/json")
     requestShooter := &http.Client{}
     resp, err := requestShooter.Do(currentRequest)
     if err == nil {

       serverSync.Lock()
       severResponse = append(severResponse, resp)
       serverSync.Unlock()
       }

     }(currentRequest)
     } //end of for

     serverWaitGroup.Wait()

     return concatenateSetServerResp(severResponse)


}


// this function creates a request pool and makes asynchronous POST request to servers
func requestFetchServers(arrReq []*http.Request) ([]byte, int) {

     // when a server is requested it should be locked so that write does not occur at same time
     var serverSync = &sync.Mutex{}
     var serverWaitGroup sync.WaitGroup

     severResponse := make([]*http.Response, 0)

     serverWaitGroup.Add(len(arrReq))

     for _, currentRequest := range arrReq {

     go func(currentRequest *http.Request)   {
     defer serverWaitGroup.Done()
     currentRequest.Header.Set("Content-type", "application/json")
     requestShooter := &http.Client{}
     resp, err := requestShooter.Do(currentRequest)
     if err == nil {

       serverSync.Lock()
       severResponse = append(severResponse, resp)
       serverSync.Unlock()
       }

     }(currentRequest)
     } //end of for

     serverWaitGroup.Wait()

     return concatenateFetchServerResp(severResponse)


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

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(resultcode)
    w.Write(result)

    // once the response is sent back. stop the timer and display it to track metrics
    fmt.Println("TOTAL TIME TAKEN FOR PUT REQUEST: %s", time.Since(start))
}


// check if there is an error in reading file
func validate(e error) {
if e != nil {
fmt.Println("Error in reading file %v", e)
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


    var kvStoreJson []keyValRequest
    json.Unmarshal(body, &kvStoreJson)

    hashModServer := make(map[int] []keyValRequest)

    for i:=0; i < len(kvStoreJson); i++ {


    keyVal := kvStoreJson[i].Key

    fmt.Printf("the key is %s\n", keyVal)
    fmt.Printf("the value is %s\n", kvStoreJson[i].Value)
    keyValStr := keyVal



    // select server by mod logic

    hashFunc := fnv.New32a()
    hashFunc.Write([]byte(keyValStr))


    findServer := int(hashFunc.Sum32()) % len(keyValServer)



    fmt.Printf("Server Id %d\n", findServer)

    // at the same time there can be multiple requests so append them
    hashModServer[findServer] = append(hashModServer[findServer], kvStoreJson[i])

    } // end of for



   // array of requests
    arrRequest := make([]*http.Request, 0)

    for i:=0; i < len(keyValServer); i++ {

            currentServer :=  hashModServer[i]

            // check if there is a request present
            fmt.Printf("Hi putting key value %d\n", len(currentServer))
            if len(currentServer) > 0 {
            fmt.Println("server request\n")
            serverEndPoint := fmt.Sprintf("http://%s:%d/set", keyValServer[i].Host, keyValServer[i].PortNum)


             httpReq := createRequest(serverEndPoint, currentServer, http.MethodPut)
                    arrRequest = append(arrRequest, httpReq)
                    } // end of if
                } // end of for

                result, resultcode := requestSetServers(arrRequest)


               w.Header().Set("Content-Type", "application/json")
                   w.WriteHeader(resultcode)
                   w.Write(result)

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



    hashModServer := make(map[int] []keyStruct)

    for i:=0; i < len(keyReceived); i++ {

    keyVal := keyReceived[i].Key
    keyValStr := keyVal




    // select server by hash mod logic

     // select server by mod logic

        hashFunc := fnv.New32a()
        hashFunc.Write([]byte(keyValStr))


        findServer := int(hashFunc.Sum32()) % len(keyValServer)


    fmt.Printf("Server Id %d\n", findServer)

    // at the same time there can be multiple requests so append them
    hashModServer[findServer] = append(hashModServer[findServer], keyReceived[i])

    } // end of for



   // array of requests
    arrRequest := make([]*http.Request, 0)

    for i:=0; i < len(keyValServer); i++ {

            currentServer :=  hashModServer[i]

            // check if there is a request present
            fmt.Printf("Hi putting key value %d\n", len(currentServer))
            if len(currentServer) > 0 {
            fmt.Println("server request\n")
            serverEndPoint := fmt.Sprintf("http://%s:%d/getkv", keyValServer[i].Host, keyValServer[i].PortNum)


             httpReq := createRequest(serverEndPoint, currentServer, http.MethodPost)
                    arrRequest = append(arrRequest, httpReq)
                    } // end of if
                } // end of for

                result, resultcode := requestFetchServers(arrRequest)


                w.Header().Set("Content-Type", "application/json")
                    w.WriteHeader(resultcode)
                    w.Write(result)

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
