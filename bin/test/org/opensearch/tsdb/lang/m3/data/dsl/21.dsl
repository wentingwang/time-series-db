{
  "size" : 0,
  "query" : {
    "time_range_pruner" : {
      "min_timestamp" : 999400000,
      "max_timestamp" : 1001000000,
      "query" : {
        "bool" : {
          "filter" : [
            {
              "range" : {
                "timestamp_range" : {
                  "from" : 999400000,
                  "to" : 1001000000,
                  "include_lower" : true,
                  "include_upper" : false,
                  "boost" : 1.0
                }
              }
            },
            {
              "terms" : {
                "labels" : [
                  "name:a"
                ],
                "boost" : 1.0
              }
            }
          ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      },
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0_unfold" : {
      "time_series_unfold" : {
        "min_timestamp" : 999400000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "moving",
            "interval" : 600000,
            "function" : "sum"
          },
          {
            "type" : "summarize",
            "interval" : 420000,
            "function" : "sum",
            "alignToFrom" : false,
            "referenceTimeConstant" : -62135596800000
          },
          {
            "type" : "truncate",
            "truncate_start" : 999960000,
            "truncate_end" : 1001000000
          }
        ]
      }
    }
  }
}

