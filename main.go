package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"bytes"

	// Import the Elasticsearch library packages
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

// Declare a struct for Elasticsearch fields
type ElasticDocs struct {
	SomeStr string
	SomeInt int
	SomeBool bool
}

// A function for marshaling structs to JSON string
func jsonStruct(doc ElasticDocs) string {
	// Create struct instance of the Elasticsearch fields struct object
	docStruct := &ElasticDocs{
		SomeStr: doc.SomeStr,
		SomeInt: doc.SomeInt,
		SomeBool: doc.SomeBool,
	}
	
	fmt.Println("\ndocStruct:", docStruct)
	fmt.Println("docStruct TYPE:", reflect.TypeOf(docStruct))
	
	// Marshal the struct to JSON and check for errors
	b, err := json.Marshal(docStruct)
	if err != nil {
		fmt.Println("json.Marshal ERROR:", err)
		return string(err.Error())
	}

	return string(b)
}

func main() {

	// Allow for custom formatting of log output
	log.SetFlags(0)
	
	// Create a context object for the API calls
	ctx := context.Background()
	
	// Create a mapping for the Elasticsearch documents
	var (
		docMap map[string]interface{}
	)

	fmt.Println("docMap:", docMap)
	fmt.Println("docMap TYPE:", reflect.TypeOf(docMap))

	// Declare an Elasticsearch configuration
	cfg := elasticsearch.Config{
		Addresses: []string{
		"http://localhost:9200",
		},
		Username: "user",
		Password: "pass",
	}
	
	// Instantiate a new Elasticsearch client object instance
	client, err := elasticsearch.NewClient(cfg)
	
	if err != nil {
		fmt.Println("Elasticsearch connection error:", err)
	}

	// Have the client instance return a response
	res, err := client.Info()

	// Deserialize the response into a map.
	if err != nil {
		log.Fatalf("client.Info() ERROR:", err)
	} else {
		log.Printf("client response:", res)
	}

	// Declare empty array for the document strings
	var docs []string

	// Declare documents to be indexed using struct
	doc1 := ElasticDocs{}
	doc1.SomeStr = "Some Value"
	doc1.SomeInt = 123456
	doc1.SomeBool = true

	doc2 := ElasticDocs{}
	doc2.SomeStr = "Another Value"
	doc2.SomeInt = 42
	doc2.SomeBool = false	

	// Marshal Elasticsearch document struct objects to JSON string
	docStr1 := jsonStruct(doc1)
	docStr2 := jsonStruct(doc2)

	// Append the doc strings to an array
	docs = append(docs, docStr1)
	docs = append(docs, docStr2)

	// Iterate the array of string documents
	for i, bod := range docs {
		fmt.Println("\nDOC _id:", i+1)
		fmt.Println(bod)
		
		// Instantiate a request object
		req := esapi.IndexRequest {
			Index: "some_index",
			DocumentID: strconv.Itoa(i + 1),
			Body: strings.NewReader(bod),
			Refresh: "true",
		}

		fmt.Println(reflect.TypeOf(req))

		// Return an API response object from request
		res, err := req.Do(ctx, client)
		if err != nil {
			log.Fatalf("IndexRequest ERROR: %s", err)
		}

		defer res.Body.Close()

		if res.IsError() {
			log.Printf("%s ERROR indexing document ID=%d", res.Status(), i+1)
		} else {
			
			// Deserialize the response into a map.
			var resMap map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&resMap); err != nil {
				log.Printf("Error parsing the response body: %s", err)
			} else {
				log.Printf("\nIndexRequest() RESPONSE:")
				// Print the response status and indexed document version.
				fmt.Println("Status:", res.Status())
				fmt.Println("Result:", resMap["result"])
				fmt.Println("Version:", int(resMap["_version"].(float64)))
				fmt.Println("resMap:", resMap)
				fmt.Println("\n")
			}
		}
	}

    // Search for the indexed document
    // Build the request body
    var buf bytes.Buffer
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "match": map[string]interface{}{
                "SomeStr": "Another",
            },
        },
    }

    if err := json.NewEncoder(&buf).Encode(query); err != nil {
        log.Fatalf("Error encoding query: %s", err)
    }

    // Perform the search request.
    res, err = client.Search(
        client.Search.WithContext(context.Background()),
        client.Search.WithIndex("some_index"),
        client.Search.WithBody(&buf),
        client.Search.WithTrackTotalHits(true),
        client.Search.WithPretty(),
    )

    if err != nil {
        log.Fatalf("Error getting response: %s", err)
    }

    defer res.Body.Close()

    if res.IsError() {
        var e map[string]interface{}
        if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
            log.Fatalf("Error parsing the response body: %s", err)
        } else {
            // Print the response status and error information.
            log.Fatalf("[%s] %s: %s",
                res.Status(),
                e["error"].(map[string]interface{})["type"],
                e["error"].(map[string]interface{})["reason"],
            )
        }
    }


    var  r map[string]interface{}
    if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
        log.Fatalf("Error parsing the response body: %s", err)
    }

    // Print the response status, number of results, and request duration.
    log.Printf(
        "[%s] %d hits; took: %dms",
        res.Status(),
        int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
        int(r["took"].(float64)),
    )

    // Print the ID and document source for each hit.
    for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
        log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	}	
	
	// Set up the request object.
	req := esapi.DeleteRequest{
        Index:      "some_index",
        DocumentID: strconv.Itoa(1),
    }

    res, err = req.Do(context.Background(), client)
    if err != nil {
      log.Fatalf("Error getting response: %s", err)
    }
}
