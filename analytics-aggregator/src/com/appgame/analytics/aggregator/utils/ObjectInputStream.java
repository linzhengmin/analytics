package com.appgame.analytics.aggregator.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 输输入字节流（'Output'和'Input'共同规范了聚合器ＣＳ两端数据序列化格式）
 */
public class ObjectInputStream extends InputStream
{
	/**
	 * 底层字节流对象（数据解压）
	 */
	private final InputStream stream;
	
	/**
	 * 流对象专用字节缓存空间
	 */
	private final byte[] buffer = new byte[8196];
	
	/**
	 * 构造方法（指定底层字节流对象）
	 */
	public ObjectInputStream(InputStream stream)
	{
		this.stream = stream;
	}
	
	/**
	 * 从字节流对象读取一个字节
	 */
	@Override
	public int read() throws IOException
	{
		return stream.read();
	}
	
	/**
	 * 从字节流对象读取一个整形数值
	 */
	public int readInt() throws IOException
	{
		stream.read(buffer, 0, Bytes.SIZEOF_INT);
		return Bytes.toInt(buffer);
	}
	
	/**
	 * 从字节流对象读取一个长整形数值
	 */
	public long readLong() throws IOException
	{
		stream.read(buffer, 0, Bytes.SIZEOF_LONG);
		return Bytes.toLong(buffer);
	}
	
	/**
	 * 从字节流对象对去一个短整形数值
	 */
	public short readShort() throws IOException
	{
		stream.read(buffer, 0, Bytes.SIZEOF_SHORT);
		return Bytes.toShort(buffer);
	}
	
	/**
	 * 从字节流对象读取一个对象
	 * @throws Exception 
	 */
	public Object readObject() throws Exception
	{
		int    len = readInt();
		byte[] buf = (len <= buffer.length) ? buffer : new byte[len];
		stream.read(buf, 0, len);
		return AggregatorUtils.b2o(buf);
	}

	/**
	 * 关闭字节流对象
	 */
	public void close() throws IOException
	{
		stream.close();
	}
}
