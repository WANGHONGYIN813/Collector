

curl -X GET "10.0.24.42:9200/gbeat-dpi-filerevert*/doc/_search" -H 'Content-Type: application/json' -d'
{
  "size":10000,
 "_source": ["FileName"],
    "query" : {
        "constant_score" : {
            "filter" : {
                "term" : {
                    "PicCheck" : "porn"
                }
            }
        }
    }
}
'|jq | grep FileName | sed 's/ //g'| cut -d ':' -f2 | cut -d'"' -f 2 > list

