package com.lingyi.data.emr.tartool;

import com.lingyi.data.emr.tartool.conf.SSConf;
import com.lingyi.data.emr.tartool.util.FsClient;
import com.lingyi.data.emr.tartool.util.TarPlug;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Project：spark-unzfile
 * @name：UnZFileMain
 * @Date：2023/12/18 10:31
 * @Filename：UnZFileMain
 */
public class TarToolMainNew {


    public static void main(String[] args) throws IOException, URISyntaxException {
        Logger log = Logger.getLogger(TarToolMainNew.class);
        Logger.getLogger("org").setLevel(Level.WARN);
        String inputPath = "";
        String outPath = null;
        String jobid = "produce.properties";
        if (args.length <= 3 && args.length >= 2) {
            inputPath = args[0];
            outPath = args.length == 3 ? args[1] : null;
            jobid = args.length == 3 ? args[2] : args[1];

        } else {
            log.error("Input parameter type: Absolute Path");
            return;
        }
//        inputPath = "file:/D:/新建文件夹/14623022.zip";
        FsClient fs = new FsClient();
        if (fs.isDirectory(inputPath)) {
            log.warn("是一个目录" + inputPath);
            Configuration conf = new Configuration();
            Path pathExists = new Path(inputPath);
            FileSystem fs1 = FileSystem.get(new URI(inputPath), conf);
            try {
                SparkContext scs = new SSConf("produce", "sparkTarTool-" + jobid, jobid).setSs().sparkContext();
                for (FileStatus fileStatus : fs1.listStatus(pathExists)) {
                    if (fs.isDirectory(fileStatus.getPath().toString())) {
                        log.error(String.format("[%s]是一个路径，不进行直接解压路径", inputPath));
                        return;
                    }
                }
                JavaSparkContext jscs = new JavaSparkContext(scs);
                JavaPairRDD<String, PortableDataStream> binaryFiles = jscs.binaryFiles(inputPath);
                String finalOutPath1 = outPath;
                binaryFiles.foreachPartition(x -> {
                    while (x.hasNext()) {
                        Tuple2<String, PortableDataStream> xx = x.next();
                        String path = xx._1();
                        PortableDataStream pds = xx._2();
                        int length = path.split("/").length;
                        String s = path.split("/")[length - 1].replaceAll("(\\.tar)|(\\.zip)|(\\.7z)|(\\.json)|(\\.jsonl)|(\\.tar)|(\\.gz)", "");
                        String out = finalOutPath1 == null ? path : finalOutPath1 + "/" + s;
                        TarPlug tarPlug = new TarPlug(out, pds);
                        tarPlug.unZFile();
                    }
                });
            } catch (InvalidInputException e) {
                log.error(e.getMessage());
            }
        } else {
            try {
                log.warn("是一个文件----" + inputPath);
                SparkContext sc = new SSConf("produce", "sparkTarTool-" + jobid, jobid).setSs().sparkContext();
                JavaSparkContext jsc = new JavaSparkContext(sc);
                JavaPairRDD<String, PortableDataStream> binaryFiles = jsc.binaryFiles(inputPath);
                String finalOutPath = outPath;
                String finalInputPath = inputPath;
                binaryFiles.foreach(x -> {
                    String path = x._1();
                    PortableDataStream pds = x._2();
                    String out = finalOutPath == null ? path : finalOutPath;
                    TarPlug tarPlug = new TarPlug(out, pds, finalInputPath);
                    tarPlug.unZFile();
                });
                jsc.close();
                log.warn(String.format("解压完成[%s]: %s", System.currentTimeMillis(), outPath));
            } catch (InvalidInputException e) {
                log.warn(e.getMessage());
            }
        }

    }
}


