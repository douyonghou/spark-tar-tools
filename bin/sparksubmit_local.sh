#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Please enter the decompression path or file"
elif [ $# -eq 1 ]; then
  # 解压到当前路径
  input_path=$1
  spark-submit \
      --class com.lingyi.data.emr.tartool.TarToolMain \
      --master local[*] \
      ./spark-tar-tool-1.0-SNAPSHOT.jar $output_path
elif [ $# -eq 2 ]; then
  # 自定义解压路径
  input_path=$1
  output_path=$2
  spark-submit \
      --class com.lingyi.data.emr.tartool.TarToolMain \
      --master local[*] \
      ./spark-tar-tool-1.0-SNAPSHOT.jar $input_path $output_path
else
  echo "If the number of parameters is greater than 2, the parameter values are as follows: $@"
fi

