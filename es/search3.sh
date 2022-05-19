curl -XGET  'ip-172-31-14-66.ec2.internal:9200/gographidx/_search' \
	  -H "content-type: application/json" \
 -d ' { "query": {
                                         "bool": {
                                           "must": [
                                            {
                                              "match": {
                                                "attr": "title"
                                                  }
                                               },
                                                {
                                                         "match": {
                                                           "graph": "m"
                                                         }
                                                   },
                                               {
                                                "query_string": {
                                                  "query": "jones AND indiana"
                                                }
                                               }
                                            ]
                                   }
                                }
                        }'
