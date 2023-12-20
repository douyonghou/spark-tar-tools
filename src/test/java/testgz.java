
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;

public class testgz {
    public static void main(String[] args) throws IOException {
        String gzFilePath = "D:\\zip.gz"; // 指定要解压的gzip文件路径

        File file = new File(gzFilePath);
        GzipCompressorInputStream inputStream = new GzipCompressorInputStream(new FileInputStream(file),true);
        String compressedFilename = GzipUtils.getCompressedFilename(gzFilePath);
        String uncompressedFilename = GzipUtils.getUncompressedFilename(gzFilePath);
        boolean compressedFilename1 = GzipUtils.isCompressedFilename(gzFilePath);
        System.out.println("filename: " + compressedFilename1);



    }
}