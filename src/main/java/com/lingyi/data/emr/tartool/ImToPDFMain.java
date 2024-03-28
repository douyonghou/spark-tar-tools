package com.lingyi.data.emr.tartool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Project：spark-unzfile
 * @name：UnZFileMain
 * @Date：2023/12/18 10:31
 * @Filename：UnZFileMain
 */
public class ImToPDFMain {


    public static void main(String[] args) throws IOException, URISyntaxException {
        Logger log = Logger.getLogger(ImToPDFMain.class);
        Logger.getLogger("org").setLevel(Level.WARN);
        String inputPath = "";
        String outPath = null;
        String jobid = "produce.properties";
/*        if (args.length <= 3 && args.length >= 2) {
            inputPath = args[0];
            outPath = args.length == 3 ? args[1] : null;
            jobid = args.length == 3 ? args[2] : args[1];

        } else {
            log.error("Input parameter type: Absolute Path");
            return;
        }*/
        inputPath = "file:/D:/新建文件夹/";
        Configuration conf = new Configuration();
        URI uri = new URI(inputPath);
        FileSystem fs = FileSystem.get(uri, conf);

        Path f = new Path(uri);
        RemoteIterator<LocatedFileStatus> listedFiles = fs.listFiles(f,true);

        while (listedFiles.hasNext()){
            LocatedFileStatus next = listedFiles.next();
            System.out.println(next.getPath().toString());
        }
        /*try {
            SparkContext scs = new SSConf("dev", "sparkTarTool-" + jobid, jobid).setSs().sparkContext();
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
        }*/

    }
}


