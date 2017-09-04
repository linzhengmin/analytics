package com.appgame.analytics.aggregator.utils;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 输出字节流（'Output'和'Input'共同规范了聚合器ＣＳ两端数据序列化格式）
 */
public class ObjectOutputStream extends OutputStream
{
	/**
	 * 底层字节流对象（数据压缩）
	 */
	private final OutputStream stream;
	
	/**
	 * 构造方法（指定底层字节流对象）
	 */
	public ObjectOutputStream(OutputStream stream)
	{
		this.stream = stream;
	}
	
	/**
	 * 将一个字节数据写入字节流对象
	 */
	@Override
	public void write(int b) throws IOException
	{
		stream.write(b);
	}
	
	/**
	 * 将一个整形数值写入字节流对象
	 */
	public void writeInt(int v) throws IOException
	{
		stream.write(Bytes.toBytes(v));
	}
	
	/**
	 * 将一个长整形数值写入字节流对象
	 */
	public void writeLong(long v) throws IOException
	{
		stream.write(Bytes.toBytes(v));
	}
	
	/**
	 * 将一个短整形数值写入字节流对象
	 */
	public void writeShort(short v) throws IOException
	{
		stream.write(Bytes.toBytes(v));
	}
	
	/**
	 * 将一个对象实例写入字节流对象
	 * @throws Exception 
	 */
	public void writeObject(Object v) throws Exception
	{
		byte[] b = AggregatorUtils.o2b(v);
		stream.write(Bytes.toBytes(b.length));
		stream.write(b);
	}
	
	/**
	 * 通知底层字节流对象刷新
	 */
	@Override
	public void flush() throws IOException
	{
		stream.flush();
	}
	
	/**
	 * 关闭字节流对象
	 */
	@Override
	public void close() throws IOException
	{
		stream.close();
	}
}
