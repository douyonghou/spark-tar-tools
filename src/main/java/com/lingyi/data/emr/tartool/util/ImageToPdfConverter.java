package com.lingyi.data.emr.tartool.util;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.util.filetypedetector.FileType;
import org.apache.pdfbox.util.filetypedetector.FileTypeDetector;

public class ImageToPdfConverter implements Serializable {
    public static void main(String[] args) throws IOException {
        HashMap<String,ByteArrayInputStream> imageMap = new  HashMap<String,ByteArrayInputStream>();
        String directoryPath = "D:\\新建文件夹\\14761421"; // 替换为你的目录路径
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(directoryPath))) {
            for (Path path : directoryStream) {
                System.out.println(path);
                File file = new File(path.toString());
                String renameStr = path.toString().replace(".pdg", ".jpg");
                System.out.println(renameStr);
                File reFile = new File(renameStr);
                file.renameTo(reFile);
                FileInputStream fis = new FileInputStream(reFile);
                byte[] byteBuff = new byte[(int) reFile.length()];
                fis.read(byteBuff);
                ByteArrayInputStream bis = new ByteArrayInputStream(byteBuff);
                imageMap.put(renameStr,bis);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        ImageToPdfConverter.convertImagesToPdf(imageMap,"");

    }
//    public static void convertImagesToPdf(ArrayList<byte[]> imageByte, String outputFile) {
    public static void convertImagesToPdf(HashMap<String,ByteArrayInputStream> imageMap, String outputFile) {
        try {
            // 创建PDDocument对象，用于存储PDF文档
            PDDocument document = new PDDocument();
            // 创建PDPage对象，用于存储PDF页面
            PDPage page = new PDPage();
            document.addPage(page);
            // 创建PDPageContentStream对象，用于绘制PDF内容
            PDPageContentStream contentStream = new PDPageContentStream(document, page);
            // 获取目录中的所有图片文件名列表
            // 将每个图片文件添加到PDF页面中
            for (String key : imageMap.keySet()) {
                // 读取图片文件并将其转换为PDImageXObject对象
                ByteArrayInputStream in = imageMap.get(key);
                FileType fileType = FileTypeDetector.detectFileType(new BufferedInputStream(in));
                System.out.println(fileType.name()+"--------fileType------------");
                if(!fileType.name().equals("JPEG"))
                    continue;
                System.out.println(key+"++++++++++");
                PDImageXObject image = PDImageXObject.createFromFile(key, document);
                // 将图片绘制到PDF页面中
                int width = image.getImage().getWidth();
                int height = image.getImage().getHeight();
                contentStream.drawImage(image, 0, 0);
                // 将内容流写入PDF页面中，并重置内容流以准备下一个页面
                contentStream.close();
                page = new PDPage();
                document.addPage(page);
                contentStream = new PDPageContentStream(document, page);
            }
            contentStream.close();
            document.save("D:\\新建文件夹\\jpg"+System.currentTimeMillis()+".pdf");
            System.out.println("-------------"+outputFile);
            document.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}