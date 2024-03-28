cd /bin/spark-tar/

input_path="tos://report/data/spider/property/log_id=1001016/stat_date=20231213/WanJuan1.0/WanJuan1.0/raw/nlp/CN/WebText-cn/"
output_path="tos://report/data/tmp"
spark-submit \
    --files ./config/wanjuan.produce.properties \
    --class com.lingyi.data.emr.tartool.TarToolMain \
    --master yarn \
    ./spark-tar-tool-1.0.8.1-SNAPSHOT.jar $input_path $output_path wanjuan.produce.properties

# 解压后验证是否都ok 文件大小为0
hadoop fs -ls $output_path | awk -F " " '{if($5=="0")print $4,$5,$8}'


input_path="tos://report/data/spider/property/log_id=1001016/stat_date=20231213/WanJuan1.0/WanJuan1.0/raw/nlp/CN/WebText-cn/"
output_path="tos://report/data/tmp"
spark-submit \
    --files ./config/wanjuan.produce.properties \
    --class com.lingyi.data.emr.tartool.TarToolMain \
    --master yarn \
    ./spark-tar-tool-1.0.8.1-SNAPSHOT.jar $input_path $output_path wanjuan.produce.properties

sh ./sparksubmit_local.sh tos://spider-01wanwu/source/读秀/1/2000-2 tos://spider-01wanwu/source/pdf/读秀/1/2000-2 paths.produce.properties