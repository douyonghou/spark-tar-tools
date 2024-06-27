package com.lingyi.data.emr.tartool.util;

import java.io.*;

public class IsReadFileU {
    public boolean isUnZstFile(String fileN, String localFileP) {
        try (BufferedReader reader = new BufferedReader(new FileReader(localFileP))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // you
                System.out.println(fileN.trim() +"-------isUnZstFile----------"+line.trim()+"-----"+line.trim().equals(fileN.trim()));
                if(line.trim().equals(fileN.trim())){
                    return true;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void addZstFile(String fileN, String localFileP) {

        // meiyou, jiu xie ru
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(localFileP, true))) {
            writer.write(fileN+"\n");
            writer.flush();
            System.out.println("The content has been added to the file.");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    public static void main(String[] args) {
        new IsReadFileU().addZstFile("aaaa", "D:\\code\\01\\spark-tar-tool\\src\\main\\java\\com\\lingyi\\data\\emr\\tartool\\util\\aaaa");
    }
}
