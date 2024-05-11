/**
 * Custom implementation of RecordReader for extracting trace records from SEGY file
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
package com.echo.readsegy.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Custom implementation of RecordReader<TraceHeaderWritable, TraceWritable> to read SEGY traces
 * 从 SEGY 文件中提取地震道（trace）记录
 */
public class TraceRecordReader extends RecordReader<TraceHeaderWritable, TraceWritable>{
	private FSDataInputStream inputStream = null;
	private long start;
    private long end;
    private long pos;
    private TraceHeaderWritable key = new TraceHeaderWritable() ;
    private TraceWritable value = new TraceWritable();
    protected Configuration conf;
    private boolean processed = false;
    private int nSamples;
    private int bytesPerSample;
    private int nFmt;

	/**
	 * Close read session
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
			inputStream.close();
	}

	/**
	 * Returns key for current record
	 * @return
	 */
	@Override
	public TraceHeaderWritable getCurrentKey()  {
		return key;
	}

	/**
	 * Returns value for current record
	 * @return
	 */
	@Override
	public TraceWritable getCurrentValue() {
		return value;
	}

	/**
	 * True if reader has already finished reading SEGY, False otherwise
	 * @return
	 */
	@Override
	public float getProgress()  {
		return ((processed == true)? 1.0f : 0.0f);
	}

	/**
	 * Initialize TraceRecordReader
	 * 初始化 TraceRecordReader。从输入的分片信息中获取起始位置、结束位置，并打开相应的输入流
	 * @param split set of split ranges
	 * @param context Task attempt context
	 * @throws IOException
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		FileSplit fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
		this.start = fileSplit.getStart();
        this.end = this.start + fileSplit.getLength();
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		this.inputStream = fs.open(path);
		inputStream.seek(this.start);
		this.pos = this.start;
		this.nSamples =  conf.getInt(SEGYInputFormat.TRACE_SAMPLES_SETTING, 3000);
		this.bytesPerSample =  conf.getInt(SEGYInputFormat.TRACE_BYTE_PER_SAMPLE_SETTING, 4);
		this.nFmt =  conf.getInt(SEGYInputFormat.TRACE_NUM_FMT_SETTING, 4);
	}

	/**
	 * Read the next value from SEGY split partition
	 * 用于读取下一个 SEGY 记录。如果当前位置小于结束位置，则从输入流中读取一个地震道的字节数组，并将其解析为 TraceWritable 对象，然后将其头信息存储在键对象中，返回 true；如果已处理完毕，则返回 false
	 * @return
	 * @throws IOException
	 */
	@Override
	public boolean nextKeyValue() throws IOException {

		if (this.pos < this.end) {
			int traceSize = SEGYInputFormat.TRACE_HEADER_SIZE + this.nSamples * this.bytesPerSample;
			byte[] traceBytes = new byte[traceSize];
			inputStream.read(traceBytes);
			//设置地震道数据样本
			value.set(traceBytes, nFmt, nSamples);
			//设置地震道的头信息
			key.set(value.getTraceHeader());
			this.pos = inputStream.getPos();
			return true;
		} else {
			processed = true;
			return false;
		}
	}
}
