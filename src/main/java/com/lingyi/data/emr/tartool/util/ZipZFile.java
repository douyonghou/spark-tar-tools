package com.lingyi.data.emr.tartool.util;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

public class ZipZFile {
    
    public static void executer(ZipArchiveEntry zae, ZipArchiveInputStream zais, String writePath, FsClient fsClient) {
        try {
            String fileNames = zae.getName().replaceAll("[/!,=| ]", "");
            System.out.println(zae.getSize() + "--------te.getSize()--------------------------");
            int size = (int) zae.getSize();
            if (zae.getSize() < (1024 * 1024 * 1000) && zae.getSize() > 0) {
                byte[] readB = new byte[size];
                zais.read(readB);
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
                        byte[] readBuf = new byte[(int) (1024 * 1024 * 6)];
                        zais.read(readBuf);
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
                } while (((read = zais.read()) != -1));
//                zais.close();
                System.out.println("Written to the tos path: " + writePath + "/" + fileNames);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    public static void main(String[] args) throws IOException {

    }
}
