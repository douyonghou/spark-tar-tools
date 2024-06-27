package com.lingyi.data.emr.tartool.util;

import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * @Project：spark-unzfile
 * @name：UnZip
 * @Date：2023/12/18 11:03
 * @Filename：UnZip
 */
public class TarPlug {
    private static final Logger log = LoggerFactory.getLogger(TarPlug.class);
    private static final String SON_PATH = "(/\\S+)";//匹配子路径
    PortableDataStream pds;
    String path;
    String inputPath;
    String zstDP;
    String localFileP;

    public TarPlug(String path, PortableDataStream pds) {
        this.pds = pds;
        this.path = path;
    }

    public TarPlug(String path, PortableDataStream pds, String inputPath, String zstDP, String localFileP) {
        this.pds = pds;
        this.path = path;
        this.inputPath = inputPath;
        this.zstDP = zstDP;
        this.localFileP = localFileP;
    }

    public void unZFile() throws IOException, URISyntaxException {

        if (this.pds.getPath().endsWith(".zip")) {
            System.out.println("decompression zip format: " + this.path);
            new TarArchive(this.path, this.pds).unZip();
        } else if (this.pds.getPath().endsWith(".warc.gz")) {
            System.out.println("decompression gzip format: " + this.path);
            new TarArchive(this.path, this.pds, this.inputPath).unWarcGzip();
        } else if (this.pds.getPath().endsWith(".tar")) {
            System.out.println("decompression tar format: " + this.path);
            new TarArchive(this.path, this.pds, this.inputPath).unTar();
        } else if (this.pds.getPath().endsWith(".bz2")) {
            System.out.println("decompression bz2 format: " + this.path);
            new TarArchive(this.path, this.pds).unBz2();
        } else if (this.pds.getPath().endsWith(".7z")) {
            System.out.println("decompression 7z format: " + this.path);
            new TarArchive(this.path, this.pds).sevenZ();
        } else if (this.pds.getPath().endsWith(".zst")) {
            System.out.println("decompression zst format: " + this.path);
            new TarArchive(this.path, this.pds).unZst(this.zstDP, this.localFileP);
        }else System.out.println("Changing format currently does not support decompression，Only tarOnly supports: tar/gz/zip/zst/7z");
    }
}
