#!/bin/bash

src=$1

if [ -z "$src" ]
then
    # fail
    echo "No source directory provided. Usage: ./bulk-import.sh <src> <out> <schema> <graphql>"
    exit 1 
fi

out=$2
if [ -z "$out" ]
then
    out="./data/out"
    echo "No output directory provided, using default: $out"
fi

schema=$3

if [ -z "$schema" ]
then
    # fail
    echo "No schema file provided. Usage: ./bulk-import.sh <src> <out> <schema> <graphql>"
    exit 1 
fi

graphql=$4

if [ -z "$graphql" ]
then
    # fail
    echo "No graphql file provided. Usage: ./bulk-import.sh <src> <out> <schema> <graphql>"
    exit 1 
fi

echo "Source: $src"
echo "Output: $out"
echo "Schema: $schema"
echo "GraphQL: $graphql"

# ask for confirmation
read -p "Are you sure you want to continue? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    # fail
    echo "Aborting..."
    exit 1
fi

# start bulk import

dgraph bulk -f $src \
	-s $schema \
	-g $graphql \
	--out "$out" \
	--map_shards=4 \
	--reduce_shards=1 \
	--zero=localhost:5080 \
	--mapoutput_mb=4096 \
	--num_go_routines=64 \
	> bulk_stdout 2> bulk_stderr & disown
