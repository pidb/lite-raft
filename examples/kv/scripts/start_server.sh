#!/bin/bash

# source shflags from current directory
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../../shflags

# define command-line flags
DEFINE_string 'host' '127.0.0.1' 'network host' 'host'
DEFINE_integer 'server_num' '3' 'Number of servers' 'server_num'
DEFINE_integer 'port' '50051' "Port of the first server" 'port'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

nodes=""
for ((i=0; i<$FLAGS_server_num; ++i)); do
    nodes="${nodes}$((i+1))=${FLAGS_host}:$((${FLAGS_port}+i)),"
done

for ((i=0; i<$FLAGS_server_num; ++i)); do
    echo $mydir/../../../target/debug/oceanraft-kv-example --addr=${FLAGS_host}:$((${FLAGS_port}+i)) --nodes ${nodes}
    $mydir/../../../target/debug/oceanraft-kv-example --addr=${FLAGS_host}:$((${FLAGS_port}+i)) --nodes ${nodes} 
done
echo $nodes
