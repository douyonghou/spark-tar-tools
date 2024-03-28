import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class SevenZByte implements Serializable {
    public void write(String inputFileL) throws IOException {
        System.out.println("解压driver端TMP下的压缩文件: " + inputFileL);

        try {
            SevenZFile sevenZFile = new SevenZFile(new File(inputFileL));
            Iterable<SevenZArchiveEntry> entries = sevenZFile.getEntries();
            entries.forEach(te -> {
                try {
                    InputStream inputStream = sevenZFile.getInputStream(te);
                    byte[] readB = new byte[1024];
                    sevenZFile.read();
                    System.out.println(new String(readB));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println("------------" + te.getName());
                        try {
                            if (!te.isDirectory()) {
                                String fileNames = te.getName().replaceAll("[!,=| ]", "");
                                int size = (int) te.getSize();
                                System.out.println(size);
                                ArrayList<Byte> bytes1 = new ArrayList<Byte>();
                                System.out.println("解压文件大小:" + te.getSize() + "-------" + size);
                                if (size > 0) {
                                    byte[] readB = new byte[1024];
                                    sevenZFile.read();
                                    sevenZFile.close();

                                } else { // byte大小超过Integer.MAX_VALUE
                                    StringBuffer stringBuffer = new StringBuffer();
                                    long i = 0;
                                    int read = sevenZFile.read();
                                    sevenZFile.close();
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }


            );
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
//        new SevenZByte().write("D:\\posts_info_20231002.7z");

        String filePath = "D:\\posts_info_20231002.7z";
        try (SevenZFile sevenZFile = new SevenZFile(new File(filePath))) {
            ArchiveEntry entry;

            AtomicInteger k = new AtomicInteger();
            sevenZFile.getEntries().forEach(x -> k.set(k.get() + 1));
            System.out.println(k);
            if(k.get()>1){
                while ((entry = sevenZFile.getNextEntry()) != null) {
                    if (!entry.isDirectory() && !entry.getName().endsWith("/")) {
                        byte[] buffer = new byte[1024*1024*512];
                        System.out.println("Extracted file: " + entry.getName());
                        int bytesRead;
                        while ((bytesRead = sevenZFile.read(buffer)) > 0) {
                            // 处理每个文件内容（这里只打印了文件名）
//                            if(!"posts_info_20231002".equals(entry.getName()))
//                                System.out.println("Extracted file: " + entry.getName());
                        }
                    }
                }
            }

            /*while ((entry = sevenZFile.getNextEntry()) != null) {
                if (!entry.isDirectory() && !entry.getName().endsWith("/")) {
                    byte[] buffer = new byte[4096];

                    int bytesRead;
                    while ((bytesRead = sevenZFile.read(buffer)) > 0) {
                        // 处理每个文件内容（这里只打印了文件名）
                        if(!"posts_info_20231002".equals(entry.getName()))
                        System.out.println("Extracted file: " + entry.getName());
                    }
                }
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
