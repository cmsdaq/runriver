
riverInstMapping = {
	"properties" : {
                "es_central_cluster": {
			"type" : "keyword"
                },
		"es_local_cluster" : {
			"type" : "keyword"
		},
		"es_local_host" : {
			"type" : "keyword"
		},
		"runindex_read" : {
			"type" : "keyword"
		},
		"runindex_write" : {
			"type" : "keyword"
		},
		"boxinfo_read" : {
			"type" : "keyword"
		},
		"polling_interval" : {
			"type" : "integer"
		},
		"fetching_interval" : {
			"type" : "integer"
		},
		"subsystem" : { #cdaq,minidaq, etc.
			"type" : "keyword"
		},
		"runNumber": { #0 or unset if main instance
			"type":"integer"
		},
		"instance_name" : { #e.g. river_cdaq_run123456, river_minidaq_main etc. (same as _id?)
			"type" : "keyword"
		},
                "process_type" : {
			"type" : "keyword"
                },
                "path" : {
			"type" : "keyword"
                },
                "role":{
			"type" : "keyword"
                },
		"node" : {
			"properties" : {
				"name" : { #fqdn
			                "type" : "keyword"
				},
				"status": { #created, crashed, running, done, stale? ...
			                "type" : "keyword"
				},
				"ping_timestamp":{ #last update (keep alive status?)
					"type":"date"
				},
				"ping_time_fmt":{ #human readable ping timestamp
					"type":"date",
					"format": "YYYY-MM-dd HH:mm:ss"
				}
			}
		},
		"errormsg" : { #crash or exception error
			"type" : "keyword"
		},
		"enable_stats" : { #write stats document
			"type" : "boolean"
		},
                "close_indices" : {
                        "type" : "boolean"
                },
                "streaminfo_per_bu" : {
                        "type" : "boolean"
                }
	}
}


