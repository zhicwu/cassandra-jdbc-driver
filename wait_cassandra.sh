#!/bin/sh
USERNAME=cassandra
PASSWORD=cassandra
KEYSPACE=system_traces
TESTCQL="desc keyspaces"

echo "Wait until Cassandra is up and ready to use..."

while ! cqlsh -u "$USERNAME" -p "$PASSWORD" -k "$KEYSPACE" -e "$TESTCQL" > /dev/null; do
    sleep 3

    echo " - Checking status..."
done

echo "Great! It's ready now!"