package com.echo.readsegy;

import java.io.IOException;
import java.io.RandomAccessFile;

public class SegyUtils {

    /**
     * 获取一共segy文件中道集数量
     * @param segyFile
     * @return
     * @throws IOException
     */
    public static long getRecordNum(RandomAccessFile segyFile) throws IOException{
        //TODO 获取每个道集中的地震道数量
        int dataPerRecord = getDataTracesPerRecord(segyFile);
        //TODO 获取每个道集中的辅助道数量
        int auxNum = getAuxTracesPerRecord(segyFile);
        //TODO 获取文件中所有的地震道数量
        long dataNum = getDataNum(segyFile);
        return dataNum / (dataPerRecord+auxNum);
    }

    /**
     * 获取一个segy文件中一共的地震道数量
     * @param segyFile
     * @throws IOException
     */
    public static long getDataNum(RandomAccessFile segyFile) throws IOException{
        //TODO 获取每一个地震道的采样点数量
        Integer samplesNumPerData = getSamplesNumPerData(segyFile);
        System.out.println("每一个地震道的采样点数量"+samplesNumPerData);
        //TODO  每个采样点的字节数
        Integer valueType = getValueType(segyFile);
        System.out.println("数据采样格式编码"+valueType);
        //TODO 计算每个地震道的大小
        int dataLength = samplesNumPerData * valueType + 240;
        //TODO 整个文件的大小
        long fileLength = segyFile.length();
        long dataAllLength = fileLength - 3600;
        //TODO 获取总共的地震道
        long dataNum = dataAllLength / dataLength;
        return dataNum;
    }

    /**
     * 获取数据采样格式编码
     * @param segyFile
     * @return
     * @throws IOException
     */
    public static Integer getValueType(RandomAccessFile segyFile) throws IOException{
        segyFile.seek(3224);
        int samplesPerTrace = segyFile.readShort();
        if(samplesPerTrace == 1){
            //4字节IBM浮点数
            return  4;
        } else if (samplesPerTrace == 2) {
            //两互补整数
            return  4;
        } else if (samplesPerTrace == 3) {
            //两互补整数
            return  2;
        } else if (samplesPerTrace == 4) {
            //4 字节带增益定点数（过时，不再使用）
            return  4;
        } else if (samplesPerTrace == 5) {
            //4字节IEEE浮点数
            return  4;
        } else if (samplesPerTrace == 6 || samplesPerTrace == 7) {
            //现在没有使用
            return  null;
        } else if (samplesPerTrace == 8) {
            //1 字节，两互补整数
            return 1;
        } else {
            return null;
        }
    }

    /**
     * 获取每个地震道的采样点数量
     * @param segyFile
     * @return
     * @throws IOException
     */
    public static Integer getSamplesNumPerData(RandomAccessFile segyFile) throws IOException{
        segyFile.seek(3220);
        int samplesPerTrace = segyFile.readShort();
        return samplesPerTrace;
    }

    /**
     * 获取每个道集辅助道数数量
     * @param segyFile
     * @return
     * @throws IOException
     */
    public static Integer getAuxTracesPerRecord(RandomAccessFile segyFile) throws IOException{
        segyFile.seek(3214);
        int auxTracesPerRecord = segyFile.readShort();
        return auxTracesPerRecord;
    }

    /**
     * 获取每个道集中的地震道数量
     * @param segyFile
     * @return
     * @throws IOException
     */
    public static Integer getDataTracesPerRecord(RandomAccessFile segyFile) throws IOException{
        segyFile.seek(3212);
        int dataTracesPerRecord = segyFile.readShort();
        return dataTracesPerRecord;
    }


    /**
     * 将4字节IBM浮点数转为double
     * @param bytes
     * @return
     */
    public static double turnIBMFloatToDouble(byte[] bytes) {
        int mantissa = ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        int exponent = bytes[0] & 0xFF;

        // 计算正负号
        int sign = 1;
        if ((exponent & 0x80) != 0) {
            sign = -1;
            exponent &= 0x7F;
        }

        // 转换为double
        double result = sign * (mantissa / Math.pow(2, 24)) * Math.pow(16, exponent - 64);

        return result;

    }

    /***
     * 将4字节IEEE浮点数转为double
     * @param bytes
     * @return
     */
    public static double turnIEEEFloatToDouble(byte[] bytes) {
        int bits = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        return Float.intBitsToFloat(bits);
    }



}
