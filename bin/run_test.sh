input_path="tos://report/data/spider/property/log_id=1001016/stat_date=20231213/WanJuan1.0/WanJuan1.0/raw/video/archives/archive_01*.tar"
output_path="tos://report/data/spider/tmp_wj/log_id=1001016/stat_date=20231213/data_type=video/"
spark-submit \
    --files ./config/wanjuan.produce.properties \
    --class com.lingyi.data.emr.tartool.TarToolMain \
    --master yarn \
    ./spark-tar-tool-1.0.3-SNAPSHOT.jar $input_path $output_path wanjuan.produce.properties

sparktar -client   wanjuan.produce.properties
