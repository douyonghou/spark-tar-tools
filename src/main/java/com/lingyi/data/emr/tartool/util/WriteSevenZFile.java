package com.lingyi.data.emr.tartool.util;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteSevenZFile {
    public static void executer(SevenZArchiveEntry te, SevenZFile sevenZFile, String writePath, FsClient fsClient) {
        try {
            String fileNames = te.getName().replaceAll("[/!,=| ]", "");
            System.out.println(te.getSize() + "--------te.getSize()--------------------------");
            int size = (int) te.getSize();
            if (te.getSize() < (1024 * 1024 * 1000) && te.getSize() > 0) {
                byte[] readB = new byte[size];
                sevenZFile.read(readB);
                System.out.println("After decompression, write to the tos path: " + writePath + "/" + fileNames);
                fsClient.write(writePath + "/" + fileNames, readB);
                System.out.println("Written to the tos path: " + writePath + "/" + fileNames);
            } else {
                String t = "";
                String lastLine = "";
                int read;
                int addK1 = 0;
                int addK2 = 0;
                do {
                    t = writePath + "/" + fileNames + "_" + System.currentTimeMillis();
                    if (!fsClient.exists(t)) {
                        byte[] readBuf = new byte[(int) (1024 * 1024 * 600)];
                        sevenZFile.read(readBuf);
                        String str = new String(readBuf, Charset.forName("UTF-8"));
                        // 找到最后一个换行符的位置
                        int lastIndex;
                        if ((lastIndex = str.lastIndexOf('\n')) != -1) {
                            fsClient.writeSb(t, str.substring(0, lastIndex), lastLine);
                            lastLine = str.substring(lastIndex + addK1);
                        } else if ((lastIndex = str.lastIndexOf("\r\n")) != -1) {
                            fsClient.writeSb(t, str.substring(0, lastIndex), lastLine);
                            lastLine = str.substring(lastIndex + addK2);
                        } else if ((lastIndex = str.lastIndexOf("\r")) != -1) {
                            fsClient.writeSb(t, str.substring(0, lastIndex), lastLine);
                            lastLine = str.substring(lastIndex + addK1);
                        } else {
                            fsClient.writeSb(t, str, lastLine);
                            lastLine = "";

                        }
                        addK1 = 1;
                        addK2 = 2;
                    } else {
                        System.out.println(String.format("Writing to an existing file (%s) is not allowed", t));
                    }
                } while ((read = sevenZFile.read()) != -1);
//                sevenZFile.close();
                System.out.println("Written to the tos path: " + writePath + "/" + fileNames);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void write(String inputFile, String writePath, FsClient fsClient) throws IOException {
        String inputFileL = "/tmp/" + inputFile.split("/")[inputFile.split("/").length - 1];
        System.out.println("Decompress the compressed file of the local disk under the extractor: " + inputFileL);
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
                        executer(te, sevenZFile, writePath, fsClient);
                    }

                }
                sevenZFile.close();
            } else { // When there are multiple files under the 7z package
                String t = "";
                while ((te = sevenZFile.getNextEntry()) != null) {
                    if (!te.isDirectory() && !te.getName().endsWith("/")) {
                        executer(te, sevenZFile, writePath, fsClient);
                    }
                }
                sevenZFile.close();
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
                System.out.println("The local disk file has been deleted: " + outPathL);
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
        WriteSevenZFile.write("inputFile", "outputDirectory", new FsClient());

        System.out.println(Integer.MAX_VALUE);
    }
}
