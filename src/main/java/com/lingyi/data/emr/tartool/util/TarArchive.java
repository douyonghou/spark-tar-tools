package com.lingyi.data.emr.tartool.util;


import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdInputStream;
import com.lingyi.data.emr.tartool.TarToolMain;
import org.apache.commons.cli.*;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.spark.input.PortableDataStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    public TarArchive() {

    }

    public static void main(String[] args) throws IOException {
        new TarArchive().unZst("","");
    }
    private static String[] preprocessCommandLineArgs(String[] args) {
        // 在这里对args进行一些预处理，比如过滤、转换等
        // 然后返回处理后的String数组
        // 注意：这里只是示例，你可能需要根据实际需求来实现这个方法
        return args; // 或者返回处理后的新数组
    }


    public void unZst(String zst, String localFileP) throws IOException {
        /*
        InputStream dict = new FileInputStream("C:\\Users\\Admin\\Desktop\\zstd\\zstdict");
        InputStream in = new FileInputStream("C:\\Users\\Admin\\Desktop\\zstd\\blogger_20231122121839_b7bd298c.1700578198.megawarc.warc.zst");
        ZstdInputStream zcis = new ZstdInputStream(in);
        */



        // initialization Dictionary path
        String zstdict_m = pds.getPath().split("/")[pds.getPath().split("/").length - 1].split("\\.")[1];
        // String zstDictPath = String.format("/ML-A100/team/data/data_project/dyh/megawarc/archiveteam_blogger_dictionary/archiveteam_blogger_dictionary_%s.zstdict", zstdict_m);
        String zstDictPath = String.format(zst + "archiveteam_blogger_dictionary_%s.zstdict", zstdict_m);
        System.out.println("zstDictPath path: " + zstDictPath);
        System.out.println("Dictionary path: " + pds.getPath());
        if (!Files.isRegularFile(Paths.get(zstDictPath))) {
            System.out.println("I can't find the dictionary: " + zstDictPath);
            return;
        }

        // initialization ZstdInputStream
        byte[] bytesDict;
        FileInputStream fis = new FileInputStream(zstDictPath);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
            bos.write(buffer, 0, bytesRead);
        }
        bytesDict = bos.toByteArray();
        ZstdInputStream zcis = new ZstdInputStream(this.pds.open());
        zcis.setDict(bytesDict);

        // Execute decompression
        FsClient fsClient = new FsClient();
        int len = 0;
        byte[] readBuf = new byte[(int) (1024 * 1024 * 10)];
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        String outPutPath = path + pds.getPath().split("/")[pds.getPath().split("/").length - 1];
        System.out.println(outPutPath+"============"+localFileP);
        if(new IsReadFileU().isUnZstFile(outPutPath, localFileP)){
            String format = String.format("You write an existing file(%s), It is not allowed", outPutPath);
            System.out.println(format);
            return;
        }

        if (fsClient.exists(outPutPath + "/")) {
            System.out.println(String.format("You fsClient.del(%s) strt", outPutPath));
            fsClient.del(outPutPath + "/");
            System.out.println(String.format("You fsClient.del(%s) end", outPutPath));
        }

        String format1 = String.format("You write an addZstFile(%s)", localFileP);
        System.out.println(format1);

        while ((len = zcis.read(readBuf)) > 0) {

            String path1 = outPutPath + "/" + System.currentTimeMillis();
            byteArrayOut.write(readBuf, 0, len);
            if (byteArrayOut.toByteArray().length > (1024 * 1024 * 300)) {

                fsClient.write(path1, byteArrayOut.toByteArray());
                byteArrayOut.reset();
            }
        }
        if (byteArrayOut.size() > 0) {
            fsClient.write(outPutPath + "/" + System.currentTimeMillis(), byteArrayOut.toByteArray());
            byteArrayOut.reset();
        }

        new IsReadFileU().addZstFile(outPutPath, localFileP);
//        fsClient.write(outPutPath+"_succ", "".getBytes());

    }

    public void unWarcGzip() throws IOException {

        GZIPInputStream gis = new GZIPInputStream(this.pds.open());
//        InputStream gzipStream = new FileInputStream("D:\\greader_20130604103429.megawarc.warc.gz");
//        GZIPInputStream gis = new GZIPInputStream(gzipStream);

        FsClient fsClient = new FsClient();
        int len = 0;
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        String lastLine = "";
        byte[] readBuf = new byte[(int) (1024 * 1024)];

        while ((len = gis.read(readBuf)) > 0) {
            String outPutPath = path + "/" + pds.getPath().split("/")[pds.getPath().split("/").length - 1] + "_" + System.currentTimeMillis();
            String context = new String(readBuf, StandardCharsets.UTF_8);
            int lastIndex;
            byte[] byteArray = byteArrayOut.toByteArray();
            System.out.println("------------------byteArray-------------------------" + byteArray.length);
            if (byteArray.length > 1024 * 1024 * 600 && (lastIndex = context.lastIndexOf("WARC/1.0")) != -1) {
                byte[] bytes = (context.substring(0, lastIndex)).getBytes();
                byteArrayOut.write(bytes);

                /*
                System.out.println("------------------reset-------------------------");
                byte[] decompressedBytes = byteArrayOut.toByteArray();
                FileOutputStream fos = new FileOutputStream("D:\\greader\\greader"+System.currentTimeMillis());
                fos.write(decompressedBytes);
                */

                fsClient.write(outPutPath, byteArray);

                byteArrayOut.reset();
                lastLine = context.substring(lastIndex);
                byteArrayOut.write(lastLine.getBytes());
            } else {
                byteArrayOut.write(readBuf, 0, len);
            }
        }
        gis.close();
    }

    public void unZipOld() throws IOException {
        // 需要密码时用这个解析
        ZipParserClient zipParserClient = new ZipParserClient();
//        zipParserClient.zipParserNoPass(this.path, this.pds);

        // 转pdf
        zipParserClient.ImageToPd(this.path, this.pds);
    }

    public void unZip() throws IOException {
        String outPutPath = path;

        ZipArchiveInputStream zais = new ZipArchiveInputStream(this.pds.open());
        ZipArchiveEntry zae = zais.getNextZipEntry();

        ArchiveEntry ae;
        while ((ae = zais.getNextEntry()) != null) {
            if (!ae.isDirectory() && !ae.getName().endsWith("/")) {
                ZipZFile.executer(zae, zais, outPutPath, new FsClient());
            }
        }
        zais.close();

    }

    public void unTar() throws IOException, URISyntaxException {
        Pattern sonPathPattern = Pattern.compile(SON_PATH);
        TarArchiveInputStream tarIn = new TarArchiveInputStream(this.pds.open(), "UTF-8");
        TarArchiveEntry nze;
        while ((nze = tarIn.getNextTarEntry()) != null) {
            String sonPathStr = nze.getName();
//            System.out.println("----sonPathStr----" + sonPathStr);
            if (nze.getSize() > 0) {
                String outPutPath = path + "_" + sonPathStr;
//                System.out.println("----outPutPath----" + outPutPath);
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
//                        System.out.println("解压到: " + outPutPath);
                    } else {
                        log.error(String.format("You write an existing file(%s), It is not allowed", outPutPath));
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
//                    System.out.println("解压到: " + outPutPath);
                } else {
                    log.error(String.format("You write an existing file(%s), It is not allowed", outPutPath));
                }
            }
        }
    }
}
