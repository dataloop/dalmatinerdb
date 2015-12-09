#!/bin/bash

# ifconfig lo0 alias 127.0.0.2  # sets up the new interface/alias
# ifconfig lo0 alias 127.0.0.3  # sets up the new interface/alias
# ifconfig lo0 alias 127.0.0.4  # sets up the new interface/alias

stop()
{
    echo "Stop node 1..."
    ./_build/dev1/rel/ddb/bin/ddb stop
    echo "Stop node 2..."
    ./_build/dev2/rel/ddb/bin/ddb stop
    echo "Stop node 3..."
    ./_build/dev3/rel/ddb/bin/ddb stop
    echo "Stop node 4..."
    ./_build/dev4/rel/ddb/bin/ddb stop
}

start()
{
    echo "Start node 1..."
    ./_build/dev1/rel/ddb/bin/ddb start && sleep 2
    echo "Start node 2..."
    ./_build/dev2/rel/ddb/bin/ddb start && sleep 2
    echo "Start node 3..."
    ./_build/dev3/rel/ddb/bin/ddb start && sleep 2
    echo "Start node 4..."
    ./_build/dev4/rel/ddb/bin/ddb start && sleep 2
}

cluster()
{
    echo "Join node 2 to node 1..."
    ./_build/dev2/rel/ddb/bin/ddb-admin cluster join dev1@127.0.0.1
    echo "Join node 3 to node 1..."
    ./_build/dev3/rel/ddb/bin/ddb-admin cluster join dev1@127.0.0.1
    echo "Join node 4 to node 1..."
    ./_build/dev4/rel/ddb/bin/ddb-admin cluster join dev1@127.0.0.1

    echo "Plan changes..."
    ./_build/dev1/rel/ddb/bin/ddb-admin cluster plan
    echo "Commit changes..."
    ./_build/dev1/rel/ddb/bin/ddb-admin cluster commit
    echo "Ring Status ..."
    ./_build/dev1/rel/ddb/bin/ddb-admin ring-status
    echo "Cluster Status ..."
    ./_build/dev1/rel/ddb/bin/ddb-admin cluster status
}

# Check the first argument for instructions
case "$1" in
    stop)
        stop
        ;;
    start)
        start
        ;;
    *)
        stop
        start
        cluster
        ;;
esac
