#!/bin/bash

# source shflags from current directory
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../../shflags

# define command-line flags
DEFINE_string 'host' '127.0.0.1' 'network host' 'host'
DEFINE_integer 'server_num' '3' 'Number of servers' 'server_num'
DEFINE_integer 'port' '50051' "Port of the first server" 'port'
DEFINE_string 'path' '/tmp' 'runtime path' 'path'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

nodes=""
for ((i=0; i<$FLAGS_server_num; ++i)); do
    nodes="${nodes}$((i+1))=${FLAGS_host}:$((${FLAGS_port}+i)),"
done

for ((i=0; i<$FLAGS_server_num; ++i)); do
    mkdir -p ${FLAGS_path}/oceanraft_runtime/log_$((i+1))
    mkdir -p ${FLAGS_path}/oceanraft_runtime/kv_$((i+1))
    echo $mydir/../../../target/debug/oceanraft-kv-example \
        --node-id=$((i+1)) \
        --addr=${FLAGS_host}:$((${FLAGS_port}+i)) \
        --nodes=${nodes} \
        --log-storage-path=${FLAGS_path}/oceanraft_runtime/log_$((i+1)) \
        --kv-storage-path=${FLAGS_path}/oceanraft_runtime/kv_$((i+1))
    $mydir/../../../target/debug/oceanraft-kv-example \
        --node-id=$((i+1)) \
        --addr=${FLAGS_host}:$((${FLAGS_port}+i)) \
        --nodes=${nodes} \
        --log-storage-path=${FLAGS_path}/oceanraft_runtime/log_$((i+1)) \
        --kv-storage-path=${FLAGS_path}/oceanraft_runtime/kv_$((i+1)) &
done
echo $nodes
