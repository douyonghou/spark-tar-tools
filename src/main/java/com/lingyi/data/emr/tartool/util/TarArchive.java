package com.lingyi.data.emr.tartool.util;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.spark.input.PortableDataStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * @Project：spark-unzfile
 * @name：UnZip
 * @Date：2023/12/18 21:03
 * @Filename：UnZip
 */
public class TarArchive {
    private static final Logger log = LoggerFactory.getLogger(TarArchive.class);
    private static final String SON_PATH = "(/\\S+)";//匹配子路径
    PortableDataStream pds;
    String path;

    public TarArchive(String path, PortableDataStream pds) {
        this.pds = pds;
        this.path = path;
    }

    public void unGzip() throws IOException {
        Pattern sonPathPattern = Pattern.compile(SON_PATH);
        TarInputStream gz = new TarInputStream(new GZIPInputStream(new ByteArrayInputStream(this.pds.toArray())));
        TarEntry te;
        while ((te = gz.getNextEntry()) != null) {
            String sonPathStr = te.getName();
            Matcher sonPathMatcher = sonPathPattern.matcher(sonPathStr);
            if (sonPathMatcher.find() && te.getSize() > 0) {
                sonPathStr = sonPathMatcher.group(0);
                String outPutPath = path + sonPathStr;
                FsClient fsClient = new FsClient();
                if (!fsClient.exists(outPutPath)) {
                    byte[] readBuf = new byte[(int) te.getSize()];
                    gz.read(readBuf);
                    fsClient.write(outPutPath, readBuf);
                    System.out.println("解压到: " + outPutPath);
                } else {
                    log.warn(String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath));
                }
            }
        }
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
        ByteArrayInputStream is = new ByteArrayInputStream(this.pds.toArray());
        TarArchiveInputStream tarIn = new TarArchiveInputStream(is, "UTF-8");
        ArchiveEntry nze;
        while ((nze = tarIn.getNextTarEntry()) != null) {
            String sonPathStr = nze.getName();
            Matcher sonPathMatcher = sonPathPattern.matcher(sonPathStr);
            if (sonPathMatcher.find() && nze.getSize() > 0) {
                sonPathStr = sonPathMatcher.group(0);
                String outPutPath = path + sonPathStr;
                FsClient fsClient = new FsClient();
                if (!fsClient.exists(outPutPath)) {
                    byte[] readBuf = new byte[(int) nze.getSize()];
                    tarIn.read(readBuf);
                    fsClient.write(outPutPath, readBuf);
                    System.out.println("解压到: " + outPutPath);
                } else {
                    log.warn(String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath));
                }
            }
        }
    }

    public void sevenZ() throws IOException, URISyntaxException {
        Pattern sonPathPattern = Pattern.compile(SON_PATH);
        System.out.println(this.pds.getPath());
        SeekableByteChannel seekableByteChannel = new SeekableByteChannel() {
            @Override
            public boolean isOpen() {
                return false;
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public int read(ByteBuffer dst) throws IOException {
                return 0;
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                return 0;
            }

            @Override
            public long position() throws IOException {
                return 0;
            }

            @Override
            public SeekableByteChannel position(long newPosition) throws IOException {
                return null;
            }

            @Override
            public long size() throws IOException {
                return 0;
            }

            @Override
            public SeekableByteChannel truncate(long size) throws IOException {
                return null;
            }
        };
        seekableByteChannel.read(ByteBuffer.wrap(this.pds.toArray()));
        long size = seekableByteChannel.size();
        System.out.println(size);
        SevenZFile sevenZFile = new SevenZFile(seekableByteChannel);
        SevenZArchiveEntry nextEntry;
        while ((nextEntry = sevenZFile.getNextEntry()) != null){
            String name = nextEntry.getName();
            System.out.println(name);
        }


    }


}
