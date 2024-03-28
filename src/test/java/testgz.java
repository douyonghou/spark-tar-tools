import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class testgz {
    public static void main(String[] args) {
        String defaultFSPath = "tos://ml-a100";
        Configuration conf = new Configuration();
        // 需要指定capy到目标用的根路径，也就是tos的桶
        String defaultFS = "^tos://(\\S+)/";
        Pattern defaultFSP = Pattern.compile(defaultFS);
        Matcher matcher = defaultFSP.matcher("tos://spider-tos-sts/");
        if(matcher.find()){
            defaultFSPath = "tos://"+matcher.group(1);

        }
        System.out.println(defaultFSPath+"---------");

    }
}