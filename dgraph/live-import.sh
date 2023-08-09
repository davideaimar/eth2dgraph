#!/bin/bash

echo "Dgraph BulkLoader Starting..."

dgraph live --alpha alpha:9080 -f /dgraph/bulk/import/