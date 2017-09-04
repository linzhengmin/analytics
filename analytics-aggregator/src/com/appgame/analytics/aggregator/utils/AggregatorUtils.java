package com.appgame.analytics.aggregator.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.Column;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.NameBytesPair;
import com.google.protobuf.ByteString;


public class AggregatorUtils
{
	/**
	 * Dynamic class loader to load filter/comparators
	 */
	private final static ClassLoader CLASS_LOADER;
	static
	{
		ClassLoader   parent = AggregatorUtils.class.getClassLoader();
		Configuration config = HBaseConfiguration.create();
		CLASS_LOADER = new DynamicClassLoader(config, parent);
	}

	/**
	 * protos.consistency >> client.consistency
	 * @param consistency
	 * @return
	 */
	public static Consistency consistency(AggregateProtos.Consistency consistency)
	{
		switch(consistency)
		{
		case STRONG :
			return Consistency.STRONG;
		case TIMELINE:
			return Consistency.TIMELINE;
		default:
			return Consistency.STRONG;
		}
	}
	
	/**
	 * client.consistency >> protos.consistency
	 * @param consistency
	 * @return
	 */
	public static AggregateProtos.Consistency consistency(Consistency consistency)
	{
		switch(consistency)
		{
		case STRONG:
			return AggregateProtos.Consistency.STRONG;
		case TIMELINE:
			return AggregateProtos.Consistency.TIMELINE;
		default:
			return AggregateProtos.Consistency.STRONG;
		}
	}
	
    /**
     * protos.filter >> client.filter	
     * @param proto
     * @return
     * @throws Exception
     */
	@SuppressWarnings("unchecked")
	public static Filter filter(AggregateProtos.Filter proto) throws Exception
	{
		try
		{
			/**
			 * 动态获取过滤器的类型
			 */
			Class<? extends Filter> clazz = (Class<? extends Filter>)Class.forName(proto.getName(), true, CLASS_LOADER);
			/**
			 * 调用'parseFrom'方法构造过滤器
			 */
			Method method = clazz.getMethod("parseFrom", byte[].class);
			if (method != null)
			{
				return (Filter)method.invoke(clazz, proto.getFilter().toByteArray());
			}
			else
			{
				 throw new IOException("Unable to locate function: parseFrom in name : " +  proto.getName());
			}
		}
		catch (Exception e)
		{
			throw new DoNotRetryIOException(e);
		}
	}
	
	/**
	 * client.filter >> protos.filter
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	public static AggregateProtos.Filter filter(Filter filter) throws Exception
	{
		AggregateProtos.Filter.Builder builder = AggregateProtos.Filter.newBuilder();
		builder.setName(filter.getClass().getName());
		builder.setFilter(ByteStringer.wrap(filter.toByteArray()));
		return builder.build();
	}
	
	/**
	 * protos.scan >> client.scan
	 * @param proto
	 * @return
	 * @throws Exception
	 */
	public static Scan scan(AggregateProtos.Scan proto) throws Exception
	{
		/**
		 * 获取扫描范围
		 */
		byte [] startRow = HConstants.EMPTY_START_ROW;
		byte [] stopRow  = HConstants.EMPTY_END_ROW;
		if (proto.hasStartRow())
		{
			startRow = proto.getStartRow().toByteArray();
		}
		if (proto.hasStopRow())
		{
			stopRow = proto.getStopRow().toByteArray();
		}
		
		/**
		 * 按指定扫描范围构造扫描器
		 */
		Scan scan = new Scan(startRow, stopRow);
		
		/**
		 * 是否在HBASE缓存扫描到的数据（默认为缓存）
		 */
		if (proto.hasCacheBlocks())
		{
			scan.setCacheBlocks(proto.getCacheBlocks());
		}
		
		/**
		 * 指定最大版本数量
		 * 1. 不调用则只返回最大版本的数据
		 * 2. 指定参数则返回不大于指定版本的数据
		 * 3. 不指定参数则返回全部版本数据
		 */
		if (proto.hasMaxVersions())
		{
			scan.setMaxVersions(proto.getMaxVersions());
		}
		
		/**
		 * ??
		 */
		if (proto.hasStoreLimit())
		{
			scan.setMaxResultsPerColumnFamily(proto.getStoreLimit());
		}
		
		/**
		 * ??
		 */
		if (proto.hasStoreOffset())
		{
			scan.setRowOffsetPerColumnFamily(proto.getStoreOffset());
		}
		
		/**
		 * ??
		 */
		if (proto.hasLoadColumnFamiliesOnDemand())
		{
		  scan.setLoadColumnFamiliesOnDemand(proto.getLoadColumnFamiliesOnDemand());
		}
		
		/**
		 * 指定数值的时间范围
		 */
		if (proto.hasTimeRange())
		{
			AggregateProtos.TimeRange timeRange = proto.getTimeRange();
			long min = 0;
			long max = Long.MAX_VALUE;
			if (timeRange.hasFrom())
			{
				min = timeRange.getFrom();
			}
			if (timeRange.hasTo())
			{
				max = timeRange.getTo();
			}
			scan.setTimeRange(min, max);
		}
		
		/**
		 * 设置扫描过滤器
		 */
		if (proto.hasFilter())
		{
			scan.setFilter(filter(proto.getFilter()));
		}
		
		/**
		 * 设置扫描过程中强制回调的缓存限制
		 */
		if (proto.hasBatchSize())
		{
		  scan.setBatch(proto.getBatchSize());
		}
		
		/**
		 * 设置最大返回字节数（默认是无限制，是否超过限制则扫描被停止呢）
		 */
		if (proto.hasMaxResultSize())
		{
			scan.setMaxResultSize(proto.getMaxResultSize());
		}
		
		/**
		 * 这是一个优化设置吗？？
		 */
		if (proto.hasSmall())
		{
		  scan.setSmall(proto.getSmall());
		}
		
		/**
		 * ??
		 */
		for (NameBytesPair attribute: proto.getAttributeList())
		{
			scan.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
		}
		
		/**
		 * 设置扫描列族信息
		 */
		if (proto.getColumnCount() > 0)
		{
			for (Column column : proto.getColumnList())
			{
				byte[] family = column.getFamily().toByteArray();
				if (column.getQualifierCount() > 0)
				{
					for (ByteString qualifier : column.getQualifierList())
					{
						scan.addColumn(family, qualifier.toByteArray());
					}
				}
				else
				{
					scan.addFamily(family);
				}
			}
		}
		
		/**
		 * 是否逆序排列扫描结果
		 */
		if (proto.hasReversed())
		{
		  scan.setReversed(proto.getReversed());
		}
		
		/**
		 * 设置操作的一致性等级
		 */
		if (proto.hasConsistency())
		{
			scan.setConsistency(consistency(proto.getConsistency()));
		}
		
		/**
		 * 设置缓存行数量
		 */
		if (proto.hasCaching())
		{
			scan.setCaching(proto.getCaching());
		}
		return scan;
	}
	
	/**
	 * client.scan >> protos.scan
	 * @param scan
	 * @return
	 * @throws Exception
	 */
	public static AggregateProtos.Scan scan(Scan scan) throws Exception
	{
		AggregateProtos.Scan.Builder builder = AggregateProtos.Scan.newBuilder();
		builder.setCacheBlocks(scan.getCacheBlocks());
		if (scan.getBatch() > 0)
		{
			builder.setBatchSize(scan.getBatch());
		}
		if (scan.getMaxResultSize() > 0)
		{
			builder.setMaxResultSize(scan.getMaxResultSize());
		}
		if (scan.isSmall())
		{
			builder.setSmall(scan.isSmall());
		}
		Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
		if (loadColumnFamiliesOnDemand != null)
		{
			builder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand.booleanValue());
		}
		builder.setMaxVersions(scan.getMaxVersions());
		TimeRange timeRange = scan.getTimeRange();
		if (!timeRange.isAllTime())
		{
			AggregateProtos.TimeRange.Builder tbuilder = AggregateProtos.TimeRange.newBuilder();
			tbuilder.setFrom(timeRange.getMin());
			tbuilder.setTo(timeRange.getMax());
			builder.setTimeRange(tbuilder.build());
		}
		Map<String, byte[]> attributes = scan.getAttributesMap();
		if (!attributes.isEmpty())
		{
			NameBytesPair.Builder abuilder = NameBytesPair.newBuilder();
			for (Map.Entry<String, byte[]> attribute: attributes.entrySet())
			{
				abuilder.setName(attribute.getKey());
				abuilder.setValue(ByteStringer.wrap(attribute.getValue()));
				builder.addAttribute(abuilder.build());
			}
		}
		byte[] startRow = scan.getStartRow();
		if (startRow != null && startRow.length > 0)
		{
			builder.setStartRow(ByteStringer.wrap(startRow));
		}
		byte[] stopRow = scan.getStopRow();
		if (stopRow != null && stopRow.length > 0)
		{
			builder.setStopRow(ByteStringer.wrap(stopRow));
		}
		if (scan.hasFilter())
		{
			builder.setFilter(filter(scan.getFilter()));
		}
		if (scan.hasFamilies())
		{
			Column.Builder columnBuilder = Column.newBuilder();
			for (Map.Entry<byte[],NavigableSet<byte []>> family: scan.getFamilyMap().entrySet())
			{
				columnBuilder.setFamily(ByteStringer.wrap(family.getKey()));
				NavigableSet<byte []> qualifiers = family.getValue();
				columnBuilder.clearQualifier();
				if (qualifiers != null && qualifiers.size() > 0)
				{
					for (byte [] qualifier: qualifiers)
					{
						columnBuilder.addQualifier(ByteStringer.wrap(qualifier));
					}
				}
				builder.addColumn(columnBuilder.build());
			}
		}
		if (scan.getMaxResultsPerColumnFamily() >= 0)
		{
			builder.setStoreLimit(scan.getMaxResultsPerColumnFamily());
		}
		if (scan.getRowOffsetPerColumnFamily() > 0)
		{
			builder.setStoreOffset(scan.getRowOffsetPerColumnFamily());
		}
		if (scan.isReversed())
		{
			builder.setReversed(scan.isReversed());
		}
		if (scan.getConsistency() == Consistency.TIMELINE)
		{
			builder.setConsistency(consistency(scan.getConsistency()));
		}
		if (scan.getCaching() > 0)
		{
			builder.setCaching(scan.getCaching());
		}
		// 默认不缓存扫描数据（除非手动设置）
		if (scan.getCacheBlocks())
		{
			builder.setCacheBlocks(scan.getCacheBlocks());
		}
		else
		{
			builder.setCacheBlocks(false);
		}
		return builder.build();		
	}
	
	/**
	 * 对象转字节数组
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static byte[] o2b(Object obj) throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream    oos = new ObjectOutputStream(bos);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		bos.close();
		return bos.toByteArray();
	}
	
	/**
	 * 字节数组转对象
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public static Object b2o(byte[] input) throws Exception
	{
		ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(input));
		Object obj = stream.readObject();
		stream.close();
		return obj;
	}
	
//	/**
//	 * 聚合应答转列表
//	 * @param response
//	 * @return
//	 * @throws Exception 
//	 */
//	@SuppressWarnings("unchecked")
//	public static AccumulatorCollection response(AggregateProtos.AggregateResponse response) throws Exception
//	{
//		
//		
//		
//		
//		ByteArrayInputStream bis = new ByteArrayInputStream(response.getData().toByteArray());
//		SnappyInputStream    sis = new SnappyInputStream(bis);
//		ObjectInputStream    ois = new ObjectInputStream(sis);
//		try
//		{
//			List<Map<String, Object>> array = new ArrayList<Map<String, Object>>();
//			int size = ois.readInt();
//			for (int i = 0; i < size; i ++)
//			{
//				array.add((Map<String, Object>)ois.readObject());
//			}
//			return array;
//		}
//		finally
//		{
//			ois.close();
//			sis.close();
//		}
//	}
//	
//	/**
//	 * 列表转聚合应答
//	 * @param values
//	 * @return
//	 * @throws Exception 
//	 */
//	public static AggregateProtos.AggregateResponse response(AccumulatorCollection values) throws Exception
//	{
//		// 对象序列化（按格式压缩数据．为了降低内存占用，不使用'ObjectOutputStream'对象）
//		Output             bos = ByteString.newOutput();
//		SnappyOutputStream sos = new SnappyOutputStream(bos);
//		sos.write(Bytes.toBytes(values.size()));
//		for (Iterator<Map<String, Object>> iterator = values.iterator(); iterator.hasNext(); )
//		{
//			byte[] value = o2b(iterator.next());
//			sos.write(Bytes.toBytes(value.length));
//			sos.write(value);
//		}
//		sos.flush();
//		sos.close();
//		// 构造应答包
//		AggregateProtos.AggregateResponse.Builder builder = AggregateProtos.AggregateResponse.newBuilder();
//		builder.setData(bos.toByteString());
//		return builder.build();
//	}
	
}
