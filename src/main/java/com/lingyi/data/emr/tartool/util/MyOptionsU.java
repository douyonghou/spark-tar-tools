package com.lingyi.data.emr.tartool.util;

import com.lingyi.data.emr.tartool.TarToolMain;
import org.apache.commons.cli.*;

public class MyOptionsU {
    public static String zStr = "";
    public static String localFileP = "";
    public static String inputPath = null;
    public static String outPath = null;
    public static String jobid = null;
    public static void getStr() {
        Options options = new Options();

        Option z = new Option("z", "zstDictPath", true, "zstDictPath");
        Option i = new Option("i", "inputPath", true, "inputPath");
        Option o = new Option("o", "outPath", true, "outPath");
        Option j = new Option("j", "jobid", true, "jobid");
        Option l = new Option("l", "localFileP", true, "localFileP");
        options.addOption(z);
        options.addOption(i);
        options.addOption(o);
        options.addOption(j);
        options.addOption(l);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, TarToolMain.customArgs);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("customArgs", options);
            System.exit(1);
        }
        if (cmd.hasOption("z")) {
            zStr = cmd.getOptionValue("z");
            System.out.println(cmd.getOptionValue("z"));
        }
        if (cmd.hasOption("i")) {
            inputPath = cmd.getOptionValue("i");
            System.out.println(cmd.getOptionValue("i"));
        }
        if (cmd.hasOption("o")) {
            outPath = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("j")) {
            jobid = cmd.getOptionValue("j");
        }
        if (cmd.hasOption("l")) {
            localFileP = cmd.getOptionValue("l");
        }

    }
}
