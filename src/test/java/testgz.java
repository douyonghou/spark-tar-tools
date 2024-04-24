import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class testgz {
    public static void main(String[] args) {
        int str = "jflsjalfdsjlkf".lastIndexOf("\r\n");
        System.out.println(str);


    }
}