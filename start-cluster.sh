#!/bin/bash

# ifconfig lo0 alias 127.0.0.2  # sets up the new interface/alias
# ifconfig lo0 alias 127.0.0.3  # sets up the new interface/alias
# ifconfig lo0 alias 127.0.0.4  # sets up the new interface/alias

stop()
{
    for i in _build/dev*/rel/ddb/bin; do
        echo "$i - stop..."
        ./$i/ddb stop
    done
}

start()
{
    for i in _build/dev*/rel/ddb/bin; do
        echo "$i - start..."
        ./$i/ddb start 
        sleep 2
    done
}

cluster()
{
    for i in _build/dev*/rel/ddb/bin; do
        echo "$i - join cluster"
        ./$i/ddb-admin cluster join dev1@127.0.0.1 
    done

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
