package com.lingyi.data.emr.tartool;

import com.lingyi.data.emr.tartool.conf.SSConf;
import com.lingyi.data.emr.tartool.util.FsClient;
import com.lingyi.data.emr.tartool.util.MyOptionsU;
import com.lingyi.data.emr.tartool.util.TarPlug;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Project：spark-unzfile
 * @name：UnZFileMain
 * @Date：2023/12/18 10:31
 * @Filename：UnZFileMain
 */
public class TarToolMain {
    private static final Logger log = LoggerFactory.getLogger(TarToolMain.class);
    public static String[]  customArgs = null;

    private static String[] preprocessCommandLineArgs(String[] args) {
        // 在这里对args进行一些预处理，比如过滤、转换等
        // 然后返回处理后的String数组
        // 注意：这里只是示例，你可能需要根据实际需求来实现这个方法
        return args; // 或者返回处理后的新数组
    }
    public static void main(String[] args) throws IOException, URISyntaxException {
        customArgs = args;
        MyOptionsU.getStr();

        String inputPath = MyOptionsU.inputPath;
        String outPath = MyOptionsU.outPath;
        String jobid = MyOptionsU.jobid;
        String zstDP = MyOptionsU.zStr;
        String localFileP = MyOptionsU.localFileP;

        System.out.println(inputPath);
        System.out.println(outPath);
        System.out.println(jobid);

        System.out.println(inputPath+"----"+outPath+"----"+jobid);
        if (jobid.isEmpty() || outPath.isEmpty() ||  inputPath.isEmpty()) {
            System.err.println("Input parameter type: Absolute Path");
            return;
        }

/*

        String inputPath = "";
        String outPath = null;
        String jobid = "produce.properties";
        if (args.length <= 5 && args.length >= 4) {
            inputPath = args[0];
            outPath = args.length == 3 ? args[1] : null;
            jobid = args.length == 3 ? args[2] : args[1];

        } else {
            System.err.println("Input parameter type: Absolute Path");
            return;
        }
*/

//        inputPath = "file:/D:/zip/14506541.zip";

        FsClient fs = new FsClient();

        if (fs.isDirectory(inputPath)) {
            System.out.println("It's a directory");
            Configuration conf = new Configuration();
            Path pathExists = new Path(inputPath);
            FileSystem fs1 = FileSystem.get(new URI(inputPath), conf);
            /*System.out.println(fs1.getScheme()+"getScheme----------------");
            for (FileStatus fileStatus : fs1.listStatus(pathExists)) {
                System.out.println(fileStatus.getPath().toString() + "getPath----------------");
            }
            System.out.println(fs1.listStatus(pathExists)+"listStatus----------------"+fs1.listStatus(pathExists).length);
            RemoteIterator<LocatedFileStatus> listedFiles = fs1.listFiles(pathExists,true);*/
            try {
//            while (listedFiles.hasNext()) {
                for (FileStatus fileStatus : fs1.listStatus(pathExists)) {
//                LocatedFileStatus fileStatus = listedFiles.next();
                    inputPath = fileStatus.getPath().toString();
                    if (fs.isDirectory(inputPath)) {
                        System.out.println(String.format("[%s]It is a path, do not directly decompress the path", inputPath));
                        continue;
                    }
//                String inputPathSon = fileStatus.getPath().getName().contains("\\.")? fileStatus.getPath().getName().split("\\.")[0]: fileStatus.getPath().getName();
                    String inputPathSon = fileStatus.getPath().getName().replaceAll("(\\.tar)|(\\.zip)|(\\.7z)|(\\.json)|(\\.jsonl)|(\\.tar)|(\\.gz)", "");

                    SparkContext sc = new SSConf("produce", "sparkTarTool-" + jobid, jobid).setSs().sparkContext();
                    JavaSparkContext jsc = new JavaSparkContext(sc);

                    JavaPairRDD<String, PortableDataStream> binaryFiles = jsc.binaryFiles(inputPath);
                    String finalOutPath = outPath;
                    System.out.println(String.format("Start decompressing[%s]: %s", System.currentTimeMillis(), inputPath));
                    String finalInputPath1 = inputPath;

                    binaryFiles.foreach(x -> {
                        String path = x._1();
                        PortableDataStream pds = x._2();
                        String out = finalOutPath == null ? path : finalOutPath + "/" + inputPathSon;
                        TarPlug tarPlug = new TarPlug(out, pds, finalInputPath1, zstDP, localFileP);
                        tarPlug.unZFile();
                    });


                    jsc.close();
                    System.out.println(String.format("Decompression completed[%s]: %s", System.currentTimeMillis(), outPath));
                }
            } catch (InvalidInputException e) {
                System.out.println(e.getMessage());
            }

        } else {
            try {
                System.out.println("It's a file");

                SparkContext sc = new SSConf("produce", "sparkTarTool-" + jobid, jobid).setSs().sparkContext();
                JavaSparkContext jsc = new JavaSparkContext(sc);
                JavaPairRDD<String, PortableDataStream> binaryFiles = jsc.binaryFiles(inputPath);
                String finalOutPath = outPath;
                System.out.println(String.format("Start decompressing[%s]: %s", System.currentTimeMillis(), inputPath));
                String finalInputPath = inputPath;
                binaryFiles.foreach(x -> {
                    String path = x._1();
                    PortableDataStream pds = x._2();
                    String out = finalOutPath == null ? path : finalOutPath;
                    TarPlug tarPlug = new TarPlug(out, pds, finalInputPath,zstDP,localFileP);
                    tarPlug.unZFile();
                });
                jsc.close();
                System.out.println(String.format("Decompression completed[%s]: %s", System.currentTimeMillis(), outPath));
            } catch (InvalidInputException e) {
                System.out.println(e.getMessage());
            }
        }

    }
}


