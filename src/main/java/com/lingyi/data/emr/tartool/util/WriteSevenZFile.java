package com.lingyi.data.emr.tartool.util;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteSevenZFile {

    public static void write(String inputFile, String writePath, FsClient fsClient) throws IOException {
        String inputFileL = "/tmp/" + inputFile.split("/")[inputFile.split("/").length - 1];
        System.out.println("解压driver端TMP下的压缩文件: " + inputFileL);

        try {
            SevenZFile sevenZFile = new SevenZFile(new File(inputFileL));
            SevenZArchiveEntry te;
            AtomicInteger k = new AtomicInteger();
            sevenZFile.getEntries().forEach(x -> k.set(k.get() + 1));
            System.out.println(k);
            if (k.get() == 1) {
                while ((te = sevenZFile.getNextEntry()) != null) {
                    System.out.println(te.getName());
                    if (!te.isDirectory() && !te.getName().endsWith("/")) {
                        String fileNames = te.getName().replaceAll("[!,=| ]", "");
                        int size = (int) te.getSize();
                        if (size > 0) {
                            byte[] readB = new byte[size];
                            sevenZFile.read(readB);
                            System.out.println("解压后写入tos路径下: " + writePath + "/" + fileNames);
                            fsClient.write(writePath + "/" + fileNames, readB);
                            sevenZFile.close();
                            System.out.println("已写到tos路径下: " + writePath + "/" + fileNames);
                        } else {
                            String t = writePath;
                            String lastLine = "";
                            int read;
                            int addK1=0;
                            int addK2=0;
                            do {
                                writePath = t + "/" + fileNames + "_" + System.currentTimeMillis();
                                if (!fsClient.exists(writePath)) {
                                    byte[] readBuf = new byte[(int) (1024 * 1024 * 1024)];
                                    sevenZFile.read(readBuf);
//                                    fsClient.write(writePath,readBuf);
                                    String str = new String(readBuf, Charset.forName("UTF-8"));
                                    // 找到最后一个换行符的位置
                                    int lastIndex;
                                    if ((lastIndex = str.lastIndexOf('\n')) != -1) {
                                        fsClient.writeSb(writePath, str.substring(0, lastIndex), lastLine);
                                        lastLine = str.substring(lastIndex + addK1);
                                    } else if((lastIndex = str.lastIndexOf("\r\n")) != -1){
                                        lastLine = str.substring(lastIndex + addK2);
                                        lastLine = str.substring(0, lastIndex);
                                    } else if ((lastIndex = str.lastIndexOf("\r")) != -1) {
                                        fsClient.writeSb(writePath, str.substring(0, lastIndex), lastLine);
                                        lastLine = str.substring(lastIndex + addK1);
                                    } else {
                                        fsClient.writeSb(writePath, str, lastLine);
                                        lastLine = "";

                                    }
                                    addK1=1;
                                    addK2=2;
                                } else {
                                    System.out.println(String.format("你写入一个已存在的文件(%s)，是不允许的", writePath));
                                }
                            }  while ((read = sevenZFile.read()) != -1);
                            sevenZFile.close();
                            System.out.println("已写到tos路径下: " + writePath + "/" + fileNames);
                        }
                    }

                }
            } else { // 7z包下有多个文件的时候
                while ((te = sevenZFile.getNextEntry()) != null) {
                    if (!te.isDirectory() && !te.getName().endsWith("/")) {
                        byte[] buffer = new byte[1024 * 1024 * 512];
                        String fileNames = te.getName().replaceAll("[!,=| ]", "");
                        System.out.println("Extracted file: " + fileNames);
                        int bytesRead;
                        long i = 0;
                        while ((bytesRead = sevenZFile.read(buffer)) > 0) {
                            // 处理每个文件内容（这里只打印了文件名）
                            i = 1024 * 1024 * 512 + i;
                            writePath = writePath + "_" + fileNames + "_" + System.currentTimeMillis();
                            if (!fsClient.exists(writePath)) {
                                byte[] readBuf = new byte[(int) (1024 * 1024 * 512)];
                                sevenZFile.read(readBuf);
                                fsClient.write(writePath, readBuf);
                                System.out.println("解压到: " + writePath);
                            } else {
                                System.out.println(String.format("你写入一个已存在的文件(%s)，是不允许的", writePath));
                            }
                        }
                    }
                }
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void down(String inputFile, FsClient fsClient) throws IOException {
        String outPathL = "/tmp/" + inputFile.split("/")[inputFile.split("/").length - 1];
        fsClient.copyToLocalFile(inputFile, outPathL);
    }

    public static void delDisk(String inputFile) throws IOException {
        String str = inputFile.split("/")[inputFile.split("/").length - 1];
        if (str.length() > 2) {
            String outPathL = "/tmp/" + str;
            File file = new File(outPathL);
            if (file.delete()) {
                System.out.println("本地磁盘文件已经删除: " + outPathL);
            }
        }

    }

    public static void main(String[] args) throws IOException {
/*        String inputFile = "tos://report/tmp/typora_64bit_v1.4.8_setup.7z";
        String outputDirectory = "tos://report/tmp/sevenZout";
        // 先下载
        WriteSevenZFile.down(inputFile, new FsClient());

        // 在本地解压写入hdfs
        WriteSevenZFile.write(inputFile, outputDirectory, new FsClient());

        WriteSevenZFile.delDisk(inputFile);*/

        System.out.println(Integer.MAX_VALUE);
    }
}
