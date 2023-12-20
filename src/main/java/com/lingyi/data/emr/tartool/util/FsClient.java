package com.lingyi.data.emr.tartool.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FsClient {
    String path;
    byte[] readBuf;

    FileStatus[] fileStatuses;

    public FileStatus[] getFileStatuses() {
        return fileStatuses;
    }

    public FsClient() {
    }

    public FsClient(String path, byte[] readBuf) {
        this.path = path;
        this.readBuf = readBuf;
    }

    public boolean isDirectory(String path){
        try {
            Configuration conf = new Configuration();
            Path pathExists = new Path(path);
            FileSystem fs = FileSystem.get(new URI(path), conf);
            if(fs.isDirectory(pathExists)){
//                this.fileStatuses = fs.listStatus(pathExists);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean exists (String path) {
        try {
            Configuration conf = new Configuration();
            Path pathExists = new Path(path);
            FileSystem fs = FileSystem.get(new URI(path), conf);
            return fs.exists(pathExists);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(String path, byte[] readBuf) {
        Configuration conf = new Configuration();
        Path writeHDFSPath = new Path(path);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(path), conf);
            fs.createNewFile(writeHDFSPath);
            FSDataOutputStream out = fs.create(writeHDFSPath);
            out.write(readBuf);
            out.flush();
            out.close();
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
