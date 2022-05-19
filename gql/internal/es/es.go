package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/GoGraph/ds"
	param "github.com/GoGraph/dygparam"
	//	"github.com/GoGraph/gql/internal/db"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	//esv7 "github.com/elastic/go-elasticsearch/v7"
)

const (
	logid      = "gqlES: "
	fatal      = true
	allofterms = " AND "
	anyofterms = " OR "
	//
)

func syslog(s string, fatal_ ...bool) {
	if len(fatal_) > 0 {
		slog.Log(logid, s, fatal_[0])
	} else {
		slog.Log(logid, s)
	}
}

var (
	cfg   esv7.Config
	es    *esv7.Client
	err   error
	idxNm = param.ESindex

	ctx context.Context = context.Background()
)

func init() {

	if !param.ElasticSearchOn {
		syslog("ElasticSearch Disabled....")
		return
	}
	cfg = esv7.Config{
		Addresses: []string{
			//	"http://ec2-54-234-180-49.compute-1.amazonaws.com:9200",
			//"http://ip-172-31-18-75.ec2.internal:9200",
			//"http://instance-1:9200",
			"http://ip-172-31-14-66.ec2.internal:9200",
		},
		// ...
	}
	es, err = esv7.NewClient(cfg)
	if err != nil {
		syslog(fmt.Sprintf("Error creating the client: %s", err))
	}

	//
	// 1. Get cluster info
	//
	res, err := es.Info()
	if err != nil {
		syslog(fmt.Sprintf("Error getting response: %s", err))
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		syslog(fmt.Sprintf("Error: %s", res.String()))
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		syslog(fmt.Sprintf("Error parsing the response body: %s", err))
	}
	// Print client and server version numbers.
	syslog(fmt.Sprintf("Client: %s", esv7.Version))
	syslog(fmt.Sprintf("Server: %s", r["version"].(map[string]interface{})["number"]))
}

//func Query(attr string, qstring string) ds.QResult {
func Query(attr string, exact bool, qrystr string) ds.QResult {

	// prepend graph short name to attribute name e.g. "m.title" as in P attribute of nodescalar. This has been superceded by Graph attribute
	// a => predicate
	// value => space delimited list of terms

	type data struct {
		Graph string
		Field string
		Query string
	}

	var buf bytes.Buffer

	// ElasticSearch DSL Query
	esQuery := ` { "query": {
					 "bool": {
					   "must": [
	    				    {
	    				      "match": {
	    				        "attr": "{{.Field}}"          
					          }
					       },
						   {
							 "match": {
							   "graph": "{{.Graph}}"          
							 }
						   },
					       {
					       "query_string": {
			    		         "query": "{{.Query}}" 
			    		      }
					       }
					    ]
				   }
				}
			}`
	esExact := ` { "query": {
					 "bool": {
					   "must": [
	    				    {
	    				      "match": {
	    				        "attr": "{{.Field}}"          
					          }
					       },
						   {
							 "match": {
							   "graph": "{{.Graph}}"          
							 }
						   },
					       {
					       "term": {
			    		         "value.keyword": "{{.Query}}" 
			    		      }
					       }
					    ]
				   }
				}
			}`
	//
	// process text template, esQuery or esExact, depending on arguments qrystr or xactstr
	{
		if !exact {
			input := data{Graph: types.GraphSN(), Field: attr, Query: qrystr}
			tp := template.Must(template.New("query").Parse(esQuery))
			err := tp.Execute(&buf, input)
			if err != nil {
				syslog(fmt.Sprintf("Error in template execute: %s", err.Error()), fatal)
			}
		} else {
			input := data{Graph: types.GraphSN(), Field: attr, Query: qrystr}
			tp := template.Must(template.New("query").Parse(esExact))
			err := tp.Execute(&buf, input)
			if err != nil {
				syslog(fmt.Sprintf("Error in template execute: %s", err.Error()), fatal)
			}
		}

	}
	fmt.Println("buf: ", buf.String())
	// Perform the search request.
	t0 := time.Now()
	res, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(idxNm),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	t1 := time.Now()
	if err != nil {
		syslog(fmt.Sprintf("Error getting response: %s", err), fatal)
	}
	defer res.Body.Close()

	syslog(fmt.Sprintf("ES Search duration: %s", t1.Sub(t0)))
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			syslog(fmt.Sprintf("Error parsing the response body: %s", err), fatal)
		} else {
			// Print the response status and error information.
			syslog(fmt.Sprintf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			), fatal)
		}
	}
	var (
		r      map[string]interface{}
		result ds.QResult
	)
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		syslog(fmt.Sprintf("Error parsing the response body: %s", err), fatal)
	}
	// package the ID and document source for each hit into db.ds.QResult.

	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {

		source := hit.(map[string]interface{})["_source"]

		pkey_ := hit.(map[string]interface{})["_id"].(string)
		pkey := pkey_[:strings.Index(pkey_, "|")]
		sortk := source.(map[string]interface{})["sortk"].(string)
		ty := source.(map[string]interface{})["type"].(string)

		dbres := ds.NodeResult{PKey: uuid.FromString(pkey), SortK: sortk, Ty: ty[strings.Index(ty, ".")+1:]}
		result = append(result, dbres)
	}

	syslog(fmt.Sprintf("es query result: %#v", result))
	return result

}
