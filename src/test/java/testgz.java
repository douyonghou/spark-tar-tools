import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;


public class testgz {
    public static void main(String[] args) throws IOException {


        String s = "fdsfsa.tarfdsfa".replaceAll("(\\.tar)|(\\.zip)|(\\.7z)|(\\.json)|(\\.jsonl)|(\\.tar)|(\\.gz)", "");
        System.out.println(s);

    }
}