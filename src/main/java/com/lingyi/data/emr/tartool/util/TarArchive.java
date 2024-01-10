package com.lingyi.data.emr.tartool.util;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.spark.input.PortableDataStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Project：spark-unzfile
 * @name：UnZip
 * @Date：2023/12/18 21:03
 * @Filename：UnZip
 */
public class TarArchive {
    private static final Logger log = LoggerFactory.getLogger(TarArchive.class);
    private static final String SON_PATH = "(/.*)|(.*\\.mp4)|(.*)";//匹配子路径
    PortableDataStream pds;
    String path;
    String inputPath;

    public TarArchive(String path, PortableDataStream pds) {
        this.pds = pds;
        this.path = path;
    }

    public TarArchive(String path, PortableDataStream pds, String inputPath) {
        this.pds = pds;
        this.path = path;
        this.inputPath = inputPath;
    }


    public void unGzip() throws IOException {
        org.apache.hadoop.shaded.org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream gis = new org.apache.hadoop.shaded.org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream(this.pds.open());
        long i = 0;
        FsClient fsClient = new FsClient();

        int read = 0;
        while ((read = gis.read()) != -1) {
            i = 1024 * 1024 * 512 + i;
            String sonPathMatcher = gis.getMetaData().getFilename();
            String outPutPath = path + "_" + sonPathMatcher + "_" + System.currentTimeMillis();
            System.out.println("_______sonPathMatcher______" + sonPathMatcher);
            if (!fsClient.exists(outPutPath)) {
                byte[] readBuf = new byte[(int) (1024 * 1024 * 512)];

                read = gis.read(readBuf);
                fsClient.write(outPutPath, readBuf);
                System.out.println("解压到: " + outPutPath);
            } else {
                log.warn(String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath));
            }
        }
        gis.close();


    }

    public void unZip() throws IOException {
        Pattern sonPathPattern = Pattern.compile(SON_PATH);
        ByteArrayInputStream is = new ByteArrayInputStream(this.pds.toArray());
        ZipArchiveInputStream zipIn = new ZipArchiveInputStream(is, "UTF-8");
        ArchiveEntry nze;
        while ((nze = zipIn.getNextZipEntry()) != null) {
            String sonPathStr = nze.getName();
            Matcher sonPathMatcher = sonPathPattern.matcher(sonPathStr);
            if (sonPathMatcher.find() && nze.getSize() > 0) {
                sonPathStr = sonPathMatcher.group(0);
                String outPutPath = path + sonPathStr;
                FsClient fsClient = new FsClient();
                if (!fsClient.exists(outPutPath)) {
                    byte[] readBuf = new byte[(int) nze.getSize()];
                    zipIn.read(readBuf);
                    fsClient.write(outPutPath, readBuf);
                    System.out.println("解压到: " + outPutPath);
                } else {
                    log.warn(String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath));
                }
            }
        }
    }

    public void unTar() throws IOException, URISyntaxException {
        Pattern sonPathPattern = Pattern.compile(SON_PATH);
        org.apache.hadoop.shaded.org.apache.commons.compress.archivers.tar.TarArchiveInputStream tarIn = new org.apache.hadoop.shaded.org.apache.commons.compress.archivers.tar.TarArchiveInputStream(this.pds.open(), "UTF-8");
        TarArchiveEntry nze;
        while ((nze = tarIn.getNextTarEntry()) != null) {
            String sonPathStr = nze.getName();
            System.out.println("----sonPathStr----" + sonPathStr);
            if (nze.getSize() > 0) {
                String outPutPath = path + "_" + sonPathStr;
                System.out.println("----outPutPath----" + outPutPath);
                FsClient fsClient = new FsClient();
                long i = 0;
                int read = 0;
                while ((read = tarIn.read()) != -1) {
                    i = 1024 * 1024 * 512 + i;
                    outPutPath = outPutPath + "_" + System.currentTimeMillis();
                    if (!fsClient.exists(outPutPath)) {
                        byte[] readBuf = new byte[(int) (1024 * 1024 * 512)];
                        read = tarIn.read(readBuf);
                        fsClient.write(outPutPath, readBuf);
                        System.out.println("解压到: " + outPutPath);
                    } else {
                        log.warn(String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath));
                    }
                }
            }
        }

    }

    public void sevenZ() throws IOException, URISyntaxException {
        String inPutPath = this.pds.getPath();
        String outPutPath = path;
        // 写磁盘
        WriteSevenZFile.down(inPutPath, new FsClient());

        // 解压磁盘文件写入hdfs
        try {
            WriteSevenZFile.write(inPutPath, outPutPath, new FsClient());
        } catch (NullPointerException e) {
            WriteSevenZFile.delDisk(inPutPath);
            throw new RuntimeException(e);
        }

        // 删除磁盘文件
        WriteSevenZFile.delDisk(inPutPath);
    }


    public void unBz2() throws IOException {
        Pattern sonPathPattern = Pattern.compile(SON_PATH);
        TarInputStream bz2In = new TarInputStream(new BZip2CompressorInputStream(new ByteArrayInputStream(this.pds.toArray())));
        TarEntry te;
        while ((te = bz2In.getNextEntry()) != null) {
            String sonPathStr = te.getName();
            Matcher sonPathMatcher = sonPathPattern.matcher(sonPathStr);
            if (sonPathMatcher.find() && te.getSize() > 0) {
                sonPathStr = sonPathMatcher.group(0);
                String outPutPath = path + sonPathStr;
                FsClient fsClient = new FsClient();
                if (!fsClient.exists(outPutPath)) {
                    byte[] readBuf = new byte[(int) te.getSize()];
                    bz2In.read(readBuf);
                    fsClient.write(outPutPath, readBuf);
                    System.out.println("解压到: " + outPutPath);
                } else {
                    log.warn(String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath));
                }
            }
        }
    }
}
