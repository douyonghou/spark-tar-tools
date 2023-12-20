#!/bin/bash

echo "Input parameter description:\n"
echo "\t-local: Local mode submission\n"
echo "\t-client: Yarn's client mode submission\n"
echo "\t-cluster: Yarn's cluster mode submission\n"
if [ $# -eq 2 ]; then
  type=$1
  input_path=$2
  if [ $type == "-local" ]; then
    sh ./sparksubmit_local.sh $input_path
  fi
  if [ $type == "-client" ]; then
    sh ./sparksubmit_client.sh $input_path
  fi
  if [ $type == "-cluster" ]; then
    sh ./sparksubmit_cluster.sh $input_path
  fi
elif [ $# -eq 3 ]; then
  type=$1
  input_path=$2
  output_path=$3
  if [ $type == "-local" ]; then
    sh ./sparksubmit_local.sh $input_path $output_path
  fi
  if [ $type == "-client" ]; then
    sh ./sparksubmit_client.sh $input_path $output_path
  fi
  if [ $type == "-cluster" ]; then
    sh ./sparksubmit_cluster.sh $input_path $output_path
  fi
else
  echo "Please enter the submission type (-local/-client/-cluster) and the path for decompression"
fi


