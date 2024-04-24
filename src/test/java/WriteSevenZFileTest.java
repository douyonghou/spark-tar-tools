import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteSevenZFileTest {
    public static void executer(SevenZArchiveEntry te, SevenZFile sevenZFile, String writePath) {
        try {
            String fileNames = te.getName().replaceAll("[/!,=| ]", "");
            System.out.println(te.getSize() + "--------te.getSize()--------------------------");
            int size = (int) te.getSize();
            if (te.getSize() < (1024 * 1024 * 1024) && te.getSize() > 0) {
                byte[] readB = new byte[size];
                sevenZFile.read(readB);
                System.out.println("After decompression, write to the tos path: " + writePath + "/" + fileNames);
            } else {
                String t = "";
                int read;
                do {
                    t = writePath + "/" + fileNames + "_" + System.currentTimeMillis();
                    byte[] readBuf = new byte[(int) (1024 * 1024)];
                    sevenZFile.read(readBuf);
                    System.setProperty("file.encoding", "UTF-8");
                    System.out.println(readBuf + "---------------------");
                    String str = new String(readBuf, Charset.forName("UTF-8"));
                    System.out.println(str);
                    System.out.println(str.getBytes() + "---------------------");
                } while ((read = sevenZFile.read()) != -1);
                System.out.println("Written to the tos path: " + writePath + "/" + fileNames);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void write(String inputFile, String writePath) throws IOException {
        String inputFileL = "D:\\download\\谷歌\\谷歌下载\\note_20240414\\aaa\\note_20240418.7z";
//        String inputFileL = "/tmp/note_20240418.7z";


        try {
            SevenZFile sevenZFile = new SevenZFile(new File(inputFileL));
            System.out.println("Decompress the compressed file of the local disk under the extractor: " + inputFileL);
            SevenZArchiveEntry te;
            AtomicInteger k = new AtomicInteger();
            sevenZFile.getEntries().forEach(x -> k.set(k.get() + 1));
            System.out.println(k);
            if (k.get() == 1) {
                while ((te = sevenZFile.getNextEntry()) != null) {
                    System.out.println(te.getName());
                    if (!te.isDirectory() && !te.getName().endsWith("/")) {
                        executer(te, sevenZFile, writePath);
                    }

                }
                sevenZFile.close();
            } else { // 7z包下有多个文件的时候
                String t = "";
                while ((te = sevenZFile.getNextEntry()) != null) {
                    if (!te.isDirectory() && !te.getName().endsWith("/")) {
                        executer(te, sevenZFile, writePath);
                    }
                }
                sevenZFile.close();
            }

        } catch (IOException e) {
            System.out.println("Decompress the compressed file of the local disk under the extractor: " + inputFileL);
            System.out.println(e.getMessage());
        }
    }


    public static void main(String[] args) throws IOException {
        WriteSevenZFileTest.write("inputFile", "outputDirectory");
    }
}
