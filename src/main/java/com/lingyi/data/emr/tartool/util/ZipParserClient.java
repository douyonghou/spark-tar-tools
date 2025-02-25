package com.lingyi.data.emr.tartool.util;

import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.inputstream.ZipInputStream;
import net.lingala.zip4j.model.LocalFileHeader;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.UnsupportedZipFeatureException;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.hadoop.shaded.org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZipParserClient {
    private static final Logger log = LoggerFactory.getLogger(ZipParserClient.class);

    // ZipParserPass
    public void ImageToPd(String path, PortableDataStream pds){
        DataInputStream open = pds.open();
        FsClient fsClient = new FsClient();
        try {
            ZipArchiveInputStream zipIn = new ZipArchiveInputStream(open, "UTF-8");
            ZipArchiveEntry nze;

            // ArrayList<byte []
            HashMap<String,ByteArrayInputStream> imageBMap = new HashMap<String,ByteArrayInputStream>();
            while ((nze = zipIn.getNextZipEntry()) != null) {
                if (nze.getSize() > 0) {
                        byte[] readBuf = new byte[(int) nze.getSize()];
                        zipIn.read(readBuf);
                    ByteArrayInputStream bis = new ByteArrayInputStream(readBuf);
                    imageBMap.put(nze.getName(),bis);
                }
            }
            System.out.println(path+"---------ImageToPdfConverter------------------------------------");
            new ImageToPdfConverter().convertImagesToPdf(imageBMap,path + ".pdf");

        } catch (IOException ee) {
            String format = "zip解压有问题，文件损坏了[" + ee.getMessage() + "]" + pds.getPath();
            System.out.println(format);
        }
    }
    public void zipParserNoPass(String path, PortableDataStream pds) {
        DataInputStream open = pds.open();
        FsClient fsClient = new FsClient();
        try {

            ZipArchiveInputStream zipIn = new ZipArchiveInputStream(open, "UTF-8");
            ZipArchiveEntry nze;

            while ((nze = zipIn.getNextZipEntry()) != null) {
                String sonPathStr = nze.getName();
                if (sonPathStr.contains("/")) {
                    int length = sonPathStr.split("/").length;
                    sonPathStr = sonPathStr.split("/")[length - 1];
                }
                String outPutPath = path + "/" + sonPathStr.replaceAll(" ", "");
                if (nze.getSize() > 0) {

                    if (!fsClient.exists(outPutPath)) {
                        byte[] readBuf = new byte[(int) nze.getSize()];
                        zipIn.read(readBuf);
                        fsClient.write(outPutPath, readBuf);
                    } else {
                        String format = String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath);
                        fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", format.getBytes());

                    }
                }
            }

        } catch (UnsupportedZipFeatureException e) {
            if (e.getMessage().contains("encryption")) {
                fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", "需要密码".getBytes());

                zipParserPass(path, pds);

            } else {
                String format = "zip解压有问题，不知道的错误[" + e.getMessage() + "]" + pds.getPath();
                fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", format.getBytes());

            }
        } catch (IOException ee) {
            String format = "zip解压有问题，文件损坏了[" + ee.getMessage() + "]" + pds.getPath();
            fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", format.getBytes());

        } catch (Exception ee) {
            String format = "zip解压有问题[" + ee.getMessage() + "]" + pds.getPath();
            fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", format.getBytes());

        }
    }

    public void zipParserPass(String path, PortableDataStream pds) {
        String[] pass = {
                "suifeng", "fenghuang", "VX:zsdxtvip", "12345678", "hsz123456", "6622Ee", "662Ee", "52gv", "28zrs", "julian", "www.eshuyuan.com", "wcpfxk&*^TDwcpfxk", "风云07E319B496B3467&%CFE14AF3", "wcpfxk&*^TDwcpfxk@8686", "huihuirenjianrenaihuajianhuakai", "多读好书", "多多读书", "以书为伴", "孔方兄沧海横流", "奥哈哈aaa123", "以书为友", "以书会友", "52GV", "53gv", "5gv", "52gz", "fesga", "Wsz123", "1234560", "75753", "8888888", "yumi007BFY", "xuexileifenghaobanyang", "毛论", "deepsea", "Yuntai", "yutai", "555gv", "3333Ee", "123", "1234", "666666", "75487", "2345", "1235", "qwer", "123456", "efgzzz", "eg123", "123321", "gdfgdfgdfghjjj4444", "ytuytiuyiouy3", "erte645567uytuytu", "fesga123", "Ferge", "www.eshuyuan.net", "ddd111", "zdfsf", "sdfsdfdsf", "rwerewrtgfddg", "fgdsretgfdbfd", "dfgryytiurtt", "jggfj", "rytr", "hrty", "rtyiuuyo", "hjkkjl", "jkl112", "uyyut4556", "rtyuio", "毛浙东", "天天学习", "fdsffdf", "sdfsdfsdf", "tyrtuoyt5", "jhjh6", "ghjhgjghj555", "oiuytrfdf55588869", "niuniu154", "45678999trtrtrtrtr", "fffffff", "nnnnnnn", "mmmmmm", "rwrertyyuyretet6556", "dfgdfgdfgdfg26565", "www5555", "Yutien", "yutien", "ccwn84237hdwkhd", "zlpxqck", "13579", "935mpirsy634", "zgwenhuazsdb", "xuelejinxian52134guoxuehuiyankuaile", "xjb516518", "badcc2", "lcga110@163.com", "天涯的心", "WJDHhxsh", "DYFYDYR", "GCQMS5678jizhou", "郭德纲救了张纪中和王宝强", "大范甘迪让更多人", "认真你就输 了", "YLSSJYYSYZQ", "qm20150404lxpz", "SWDLQwangxiaobo", "esy", "gxxehjh", "HTYSfhcqZXMZFbymgjjmf", "3453453453453434", "87654321", "ssdd2013", "fsJDSS3", "akichina2015", "HNLLRsfnsSGH", "HM2729jyh293XSJ", "sddfrtry56hfgswr4tgdvdsda", "23545345346345334", "2342342343234", "刚毅.狮子心", "dfgdfgdfhghjhkioluiopio", "457871345654", "353467tertdrgdfg", "gfddgdfghjkhjk6269", "老程书库", "后掌书房", "天天读书哭", "dfsdfdsfds55555", "ddd555666", "ddd111222", "eee333", "4444444eeeee", "ttt888", "rgth", "yhjuiklo", "p;lo", "xz123", "热爱书园", "hahaha", "foxaaron", "julianyoung", "ZWZGQJEYHTEH5789658326987jkdcdfgtazZGFLHZZZT", "c867f7d5c9b44760519b3a4ffabfd84b", "c8f4e136b7a328867a82149021cea9d0", "cd38f6275fffd00f41726311bb6b7a5f", "f00b1c5b49df6e9b95064b1518706381", "ff378a3167a35335e320dc7d90652334", "F90315379E77760EE4FAB1916DAE84D6", "F33002B42D45F551CFC208BBC932F286", "64916565d10cba76a97ce9db9f40d957", "d247888e0048096c6d1bc9eb129b3e6f", "d6247f4c4f2caf2bb691c85109200f99", "d9289a11454320a54d1d973bf8f4315f", "02293048c80af2325b101fe51edca57a", "171077796a81b47d3c42c52dbac1d5dc", "295280df3927be407120d45456637c35", "3016f9cc97613122ee1ef67bdfb0fb4a", "34e1cfa99379ae9e819492a926b8576a", "3a84334169ad06ea2d4406b3811306a5", "4ff3b559833bff133c044d1aa4961c1a", "6baac99bab72711a1242b7f33c14db42", "7c08d5969530c4750057e6f1e7524498", "833a82081923d1e4c48d5bed9837572c", "872c8e99c6e449487e6421ee29fd5a77", "887818d87132250b88d660eb8032518d", "99320f53aeac97ed54f5fabdbc63d716", "ec80b73075fb05982c89869424e9be5f", "a80818325ce01bc94c938a8f6c2429bf", "aadbf3be35b87659771eb6687dc93622", "ab67ccce04735f95aedf81874019cfab", "afac2a419ff07dfe7ed9dd7d6ae632fa", "A7E544982C9611AA27B5332B36DD1F08", "A991A2C8747F0D19A6D2367BEE0E19B2", "be62ad5c350c400858de9c7bf281ccab", "54654756856754654634", "E书园", "你需要的一切&纪念品", "内部资料请勿传播", "www.foundpdf.com", "www.ddddffffff", "sdfg###", "eeeeee#########", "wszcdgf123", "serdfhg1233", "eresdf666", "eeefffff", "345efsf", "wwwwwwddddd", "rrrrrrrrrrrrr", "zxxzx", "dfdfe13", "wert456", "asdfg2335", "sdfghj555", "xsh20132052@%", "eshuyuan.net", "[www.eshuyuan.net]", "eshuyuan.com", "%", "BSQY2007", "zia08812", "起风了fdsg@$", "147258369", "12345", "杰尔马66", "国学数典", "继￥#@续￥*发&*几#@&本", "dushubaoku", "www.eshuyuan.net_by", "dsds0831", "efg111", "efg", "解压密码：www.cxacg.vip", "大O饿O龙", "mucynav.com", "千秋漫雪", "atx123", "马忽悠", "long", "123450", "扶她奶茶www.futanaicha.club", "1205794039", "1105", "36.5℃", "www.cxacg.vip", "mayuyu123", "四散的尘埃", "acg和谐区", "acgzone.us", "http://acgzone.us/", "acgzone.tk", "琉璃神社", "hacg.me", "节操粉碎机", "i-ab.com", "www.tianshi2.com", "galgame：ACG游戏开发部", "galgame:mobi.acgfuture.com", "光影交错的时空", "扶她奶茶", "樱花树下实现", "yhsxsx", "没有节操的灵梦", "毛玉mouyu", "e46852s", "傲娇零：", "aojiao.org", "黙示", "天照大御神", "猫与好天气", "lifaner.com", "456black", "moe", "动漫本子吧", "里番儿", "lsmj", "良时美景", "674434350", "psp.duowan.com", "御宅同萌", "tangtang", "9999", "999", "士凛密码", "渺", "通宵狂魔技术宅", "benzi", "Q10", "malow005", "www.idanmu.com", "名字就是", "827283516", "ce", "绅士党", "hentai.", "tianshi2", "妮妙", "发条奶茶", "七曜苏醒", "yaoying", "gg", "air", "爱有缘有份", "softpoint", "YES", "我没有节操", "拉杰尔的图书馆", "20131225", "RoC_1112@eyny", "tianshi2.com", "180998244", "ntr", "CR48", "inori", "BQ510", "120505478", "社会主义歼星炮", "技术宅", "YL714", "八幡hachiman", "yomishibai", "galacg.me", "ky8787", "www.diaosiqingnian", "终点", "say花火", "acgmoon", "804423314", "北+7355608", "channelc.yeah.net", "comicz.yeah.net", "kcomic.yeah.net", "www.comicer.com", "www.dqcx.net", "xcomic.yeah.net", "zsdxtvip"
        };
        FsClient fsClient = new FsClient();
        for (String pas : pass) {
            try {
                DataInputStream pds1 = pds.open();
                ZipInputStream zipIn = new ZipInputStream(pds1, pas.toCharArray());
                LocalFileHeader nze;
                while ((nze = zipIn.getNextEntry()) != null) {
                    String sonPathStr = nze.getFileName();
                    if (sonPathStr.contains("/")) {
                        int length = sonPathStr.split("/").length;
                        sonPathStr = sonPathStr.split("/")[length - 1];
                    }
                    String outPutPath = path + "/" + sonPathStr.replaceAll(" ", "");
                    if (nze.getCompressedSize() > 0 && !fsClient.exists(outPutPath)) {
                        byte[] readBuf = new byte[(int) nze.getCompressedSize()];
                        zipIn.read(readBuf);
                        fsClient.write(outPutPath, readBuf);
                    } else {
                        String format = String.format("你写入一个已存在的文件(%s)，是不允许的", outPutPath);
                        fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", format.getBytes());

                    }
                }
                break;
            } catch (UnsupportedZipFeatureException e) {
                String s = "zip解压有问题，估计是压缩文件有问题[" + e.getMessage() + "]" + pds.getPath();
                fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", s.getBytes());


            } catch (ZipException e) {
                String s = "zip解压需要密码，重试需要密码[" + e.getMessage() + "]" + pds.getPath();
                fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", s.getBytes());

            } catch (IOException ee) {
                String s = "zip解压有问题，文件损坏了[" + ee.getMessage() + "]" + pds.getPath();
                fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", s.getBytes());

            } catch (Exception ee) {
                String s = "zip解压有问题[" + ee.getMessage() + "]" + pds.getPath();
                fsClient.write("tos://spider-01wanwu/tmp/log/" + path.replaceAll("tos://spider-01wanwu/source/pdf/","")+System.currentTimeMillis()+".log", s.getBytes());

            }
        }

    }
}
