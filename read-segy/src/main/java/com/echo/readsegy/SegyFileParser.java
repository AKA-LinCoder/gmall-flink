package com.echo.readsegy;

import com.echo.readsegy.utils.SEGYInputFormat;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class SegyFileParser {
    public static void main(String[] args) {
        String filePath = "path/to/your/segy/file.segy"; // 默认路径
        // 获取资源的路径
        try {
            ClassLoader classLoader = SegyFileParser.class.getClassLoader();
            File file = new File(classLoader.getResource("viking_small.segy").getFile());
            filePath = file.getAbsolutePath();
        } catch (NullPointerException e) {
            System.out.println("SEGY文件未找到");
        }

        try (RandomAccessFile segyFile = new RandomAccessFile(filePath, "r")) {
            //TODO 获取每个道集的数据道数
            long recordNum = SegyUtils.getRecordNum(segyFile);
            System.out.println("一共有道集"+recordNum);
            getSamples(segyFile);
            parseSegyFile(segyFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void parseSegyFile(RandomAccessFile segyFile) throws IOException {
        System.out.println(segyFile.length());
        long length = segyFile.length();

        ///TODO 读3200位
        byte[] bytes = new byte[3200];
        segyFile.read(bytes, 0, 3200);
        Charset ebcdicCharset = Charset.forName("IBM1047");
        String mm =  new String(bytes, ebcdicCharset);
        System.out.println(mm);
        ///TODO 读400位
        read400Info(segyFile);

        ///TODO 读240位
        segyFile.seek(3600);
        byte[] bytes12 = new byte[240];
        segyFile.read(bytes12,0,240);

        ByteBuffer bbc = ByteBuffer.wrap(bytes12);
        bbc.position(0);
        System.out.println("240零零落落 ="+bbc.getInt());
        bbc.position(4);
        System.out.println("240零零落落 ="+bbc.getInt());
        bbc.position(8);
        System.out.println("240零零落落 ="+bbc.getInt());
        bbc.position(12);
        System.out.println("240零零落落 ="+bbc.getInt());
        bbc.position(36);
        System.out.println("240零零落落 ="+bbc.getInt());
//        ///TODO 读其余数据
        readSample(segyFile);


    }


    public static void  readSample(RandomAccessFile segyFile) throws IOException{
        int numTraces = 480;
        int samplesPerTrace = 600;
        segyFile.seek(3600+240);
        double[][] seismicData = new double[numTraces][samplesPerTrace];
        // 逐个读取地震道数据
        for (int i = 0; i < numTraces; i++) {
            for (int j = 0; j < samplesPerTrace; j++) {
                // 读取地震道数据，假设是 IEEE 浮点数格式
//                float sampleValue = segyFile.readFloat();
                byte[] sampleBytes = new byte[4]; // IBM浮点数是4字节
                segyFile.read(sampleBytes);
                // 解析IBM浮点数
                double sampleValue = SegyUtils.turnIBMFloatToDouble(sampleBytes);
                seismicData[i][j] = sampleValue;
            }
        }

        // 关闭文件
        segyFile.close();

        // 打印读取的地震道数据
        for (int i = 0; i < numTraces; i++) {
            for (int j = 0; j < samplesPerTrace; j++) {

                if(i==0){
                    System.out.println(String.format("%.3f", seismicData[i][j]));
                }

            }
            System.out.println();
        }
    }

    /**
     * 读取400 地方的数据
     * @param segyFile
     * @return
     */
    public static boolean read400Info(RandomAccessFile segyFile) throws IOException {
        System.out.println(segyFile.length());
        segyFile.seek(3200);
        byte[] bytes1 = new byte[400];
        segyFile.read(bytes1,0,400);

        ByteBuffer bb = ByteBuffer.wrap(bytes1);
        bb.position(0);
        System.out.println("作业标识号 ="+bb.getInt());
        bb.position(4);
        System.out.println("测线号 ="+bb.getInt());
        bb.position(8);
        System.out.println("卷号 ="+bb.getInt());
        bb.position(12);
        System.out.println("每个道集的数据道数 ="+bb.getShort());
        bb.position(14);
        System.out.println("5 ="+bb.getShort());
        bb.position(16);
        System.out.println("6 ="+bb.getShort());
        bb.position(18);
        System.out.println("7 ="+bb.getShort());
        bb.position(20);
        int numTraces = bb.getShort(); // 从位置 3220 处读取地震道数量
        System.out.println("地震道数量 ="+numTraces);
        bb.position(22);
        int samplesPerTrace = bb.getShort(); // 从位置 3224 处读取每个地震道的采样点数
        System.out.println("每个地震道的采样点数 ="+samplesPerTrace);

        //每个地震道的大小=数据道的采样点数×每个采样点的字节数
        //600 * 4 = 2400 2400+240 = 2640  每个地震道的大小
        // 1270800-3600 = 1267200 地震道的总长度
        //1267200/640 = 480 总共的地震道数量
        // 一个道集包含了480个地震带
        //所以就一共有一个道集


        //600*4  1270800
        return false;
    }


    /**
     * 计算 SEG Y 文件中的地震道数
     * @param segyFile
     * @throws IOException
     */
    public  static void getSamples(RandomAccessFile segyFile) throws IOException {
        //TODO 读取 SEG Y 文件头中的每个道集的数据道数字段。
        segyFile.seek(3212);
        int samplesPerTrace = segyFile.readShort();
        //TODO 计算 SEG Y 文件的总字节数（通常是通过读取文件大小或者从文件头中获取）。
        long length = segyFile.length();
        //TODO 计算每个道集数据所占的字节数：每个道集的数据道数乘以每个数据点的字节数。
        segyFile.seek(3224);
        int dataType = segyFile.readShort();
        long bytesPerTrace = samplesPerTrace * dataType;
        //计算每个道集所含的地震道数：总字节数除以每个道集数据所占的字节数。
        long l = length / bytesPerTrace;
        System.out.println("每个道集所包含的地震道数"+l);
        //将所有道集所含的地震道数相加，得到文件中的地震道数。
    }


    public static void getTrace(RandomAccessFile segyFile) throws IOException{
        //TODO 读取 SEG Y 文件头中的每个道集的数据道数字段。
        segyFile.seek(3212);
        int samplesPerTrace = segyFile.readShort();
        System.out.println("每个道集的数据道数字段"+samplesPerTrace);
        //TODO 计算 SEG Y 文件的总字节数（通常是通过读取文件大小或者从文件头中获取）。
        long length = segyFile.length();
        //TODO 计算每个道集数据所占的字节数：每个道集的数据道数乘以每个数据点的字节数。
        segyFile.seek(3224);
        int dataType = segyFile.readShort();
        long bytesPerTrace = samplesPerTrace * dataType;
        //TODO 用总字节数除以每个道集数据所占的字节数，得到道集数。
        long l = length / bytesPerTrace;
        System.out.println("道集数"+l);
    }







}
