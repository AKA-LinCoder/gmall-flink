/**
 * Custom writable object for trace record (header, data samples)
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
package com.echo.readsegy.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Custom writable implementation for a seismic trace
 * 定义了一个名为 TraceWritable 的自定义可写对象，用于表示地震数据中的地震道（trace）记录，包括道头信息和数据样本
 */
public class TraceWritable implements Writable {

    //存储地震道的头信息
    private TraceHeaderWritable traceHeader;
    //存储地震道的数据样本
    private DoubleWritable[] traceData;

    //default constructor for (de)serialization
    public TraceWritable() {
        traceHeader = new TraceHeaderWritable();
        traceData = new DoubleWritable[0];
    }

    /**
     * Serialize TraceWritable
     * 用于将 TraceWritable 对象序列化到输出流中
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        traceHeader.write(dataOutput);
        dataOutput.writeInt(traceData.length);
        for(int i=0; i<traceData.length; i++){
            traceData[i].write(dataOutput);
        }
    }

    /**
     * Deserialize TraceWritable
     * 用于从输入流中反序列化数据填充 TraceWritable 对象
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        traceHeader.readFields(dataInput);
        int size = dataInput.readInt();
        traceData = new DoubleWritable[size];
        for(int i=0; i<traceData.length; i++){
            double v = dataInput.readDouble();
            traceData[i] = new DoubleWritable(v);
        }
    }

    /**
     * Get a trace header info
     * @return
     */
    public TraceHeaderWritable getTraceHeader() {
        return traceHeader;
    }

    /**
     * Set a trace header info
     * @param traceHeader
     */
    public void setTraceHeader(TraceHeaderWritable traceHeader) {
        this.traceHeader = traceHeader;
    }

    /**
     * Returns a DoubleWritable array of data samples
     * @return
     */
    public DoubleWritable[] getTraceData(){
        return traceData;
    }

    /**
     * Returns byte array with trace data samples
     * @return
     */
    public byte[] getTraceDataBytes(){
        int bufSize =  traceData.length * Double.BYTES;
        byte[] buffer = new byte[bufSize];
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        for(int i=0; i<traceData.length;i++){
            bb.putDouble(traceData[i].get());
        }
        return bb.array();
    }

    /**
     * Returns a double array with trace data samples
     * @return
     */
    public double[] getTraceDataDouble(){
        double[] val = new double[traceData.length];
        for(int i  = 0; i < traceData.length; i++){
            val[i] = traceData[i].get();
        }
        return val;
    }

    /**
     * Set a DoubleWritable array with trace data samples
     * @return
     */
    public void setTraceData(DoubleWritable[] traceData){
        this.traceData = traceData;
    }

    /**
     * Initialize TraceWritable from byte array, given a number format and data samples per trace
     * 从字节数组中初始化 TraceWritable 对象
     * @param traceBytes trace byte array
     * @param nFmt SEGY number format
     * @param nSamples data samples per trace
     * @throws IOException
     */
    public void set(byte[] traceBytes, int nFmt, int nSamples) throws IOException {

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(traceBytes));

        byte[] traceHeaderBytes = new byte[SEGYInputFormat.TRACE_HEADER_SIZE];

        dis.read(traceHeaderBytes);

        traceHeader.fromBytes(traceHeaderBytes);
        traceData = new DoubleWritable[nSamples];

        for(int i = 0; i < nSamples; i++){
            traceData[i] = new DoubleWritable(NumFormatUtil.readFrom(nFmt, dis));
        }
    }

    /**
     * String representation
     * @return
     */
    @Override
    public String toString() {
        return traceHeader.toString() + ", TraceData[...]";
    }
}
