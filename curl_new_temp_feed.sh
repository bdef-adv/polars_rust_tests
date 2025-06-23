#!/bin/bash

curl http://localhost:3420/feeds --json '{
            "feed_name": "Test",
            "type": "filter",
            "origin": "A",
            "filters": [
                {
                    "filter_name": "duplicate_values",
                    "parameters": {
                        "column": "value"
                    }
                }
            ]
        }'