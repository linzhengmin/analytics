package com.appgame.analytics.aggregator.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyOutputStream;

import com.appgame.analytics.aggregator.accumulator.Accumulator;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.AggregateRequest;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.AggregateResponse;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.AggregateService;
import com.appgame.analytics.aggregator.utils.AggregatorUtils;
import com.appgame.analytics.aggregator.utils.MemoryChecker;
import com.appgame.analytics.aggregator.utils.ObjectOutputStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ByteString.Output;

public class Aggregator extends AggregateService implements CoprocessorService, Coprocessor
{
	/**
	 * 日志记录器
	 */
	private static Logger log = Logger.getLogger(Aggregator.class);
	
	/**
	 * RegionServer 环境
	 */
	private RegionCoprocessorEnvironment env;
	
	/**
	 * RegionServer 启动
	 */
	@Override
	public void start(CoprocessorEnvironment env) throws IOException
	{
		if (env instanceof RegionCoprocessorEnvironment)
		{
			this.env = (RegionCoprocessorEnvironment)env;
		}
		else
		{
			throw new CoprocessorException("must be loaded on a table region!");
		}
	}
	
	/**
	 * RegionServer 停止
	 */
	@Override
	public void stop(CoprocessorEnvironment env) throws IOException
	{
	}

	/**
	 * 返回当前服务对象
	 */
	@Override
	public Service getService()
	{
		return this;
	}

	/**
	 * 聚合操作
	 * @throws Throwable 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void aggregate(RpcController controller, AggregateRequest request, RpcCallback<AggregateResponse> done)
	{
		InternalScanner   scanner     = null;
		AggregateResponse response    = null;
		Accumulator       accumulator = null;
		MemoryChecker     checker     = new MemoryChecker(0.1);
		long              counter     = 0;
		try
		{
			// 构造累积器
			accumulator = Accumulator.build(request.getPipes());
			// 操作计时
			long now = System.currentTimeMillis();
			log.info("aggregate - start ...... ");
			// 遍历执行扫描操作
			for (int i = 0; i < request.getScansCount(); ++i)
			{
				// 内部扫描器对象
				scanner = env.getRegion().getScanner(AggregatorUtils.scan(request.getScans(i)));
				// 读取扫描结果并推送到累积器
				List<Cell> results = new ArrayList<Cell>();
				boolean    more    = true;
				while(more)
				{
					// 内存检查（增加检查计数）
					if ((++counter % 1000 == 0) & checker.exceed())
					{
						throw new Exception(String.format("region[%s] : not enough memory!![%d : %d]", env.getRegionInfo().getRegionNameAsString(),
																									   Runtime.getRuntime().totalMemory(),
																									   Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()));
					}
					// 从扫描器读取一行记录
					more = scanner.next(results);
					// 将一行记录推送到累积器
					if (!results.isEmpty())
					{
						Map<String, Object> map = new HashMap<String, Object>();
						for (Cell cell : results)
						{
							// 获取所属列族名称
							String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
							// 记录列族对应字段内容
							Map<String, Object> qualifiers = (Map<String, Object>)map.get(family);
							if (qualifiers == null)
							{
								qualifiers = new HashMap<String, Object>();
								map.put(family, qualifiers);
							}
							qualifiers.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
										   Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset()    , cell.getValueOffset() + cell.getValueLength()));
						}
						try
						{
							accumulator.put(map);
						}
						catch(Exception e)
						{
							results.clear();
							throw new Exception(e.getMessage() + " : input = [" + map.toString() + "]", e);
						}
						finally
						{
							map.clear();
							map = null;
						}
					}
					// 清理缓存
					results.clear();
				}
				// 清理内部扫描器
				try
				{
					scanner.close();
				}
				catch(Exception e)
				{
					log.error(e.getMessage(), e);
				}
			}
			
			/**
			 * 序列化聚合结果（按格式压缩数据．为了降低内存占用，不使用'ObjectOutputStream'对象）
			 */
			AccumulatorCollection values = accumulator.get();
			Output                bos    = ByteString.newOutput();
			SnappyOutputStream    sos    = new SnappyOutputStream(bos);
			ObjectOutputStream    oos    = new ObjectOutputStream(sos);
			oos.writeInt(values.size());
			for (Iterator<Map<String, Object>> iterator = values.iterator(); iterator.hasNext(); )
			{
				// 内存检查
				if ((++counter % 1000 == 0) & checker.exceed())
				{
					oos.close();
					throw new Exception(String.format("region[%s] : not enough memory!![%d : %d]", env.getRegionInfo().getRegionNameAsString(),
																								   Runtime.getRuntime().totalMemory(),
																								   Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()));
				}
				// 压缩并序列化指定对象
				oos.writeObject(iterator.next());
			}
			oos.flush();
			oos.close();
			// 构造聚合结果
			AggregateProtos.AggregateResponse.Builder builder = AggregateProtos.AggregateResponse.newBuilder();
			response = builder.setData(bos.toByteString()).build();
			log.info(String.format("aggregate - over[%d] ...... ", System.currentTimeMillis() - now));
		}
		catch (Throwable e)
		{
			// 记录异常信息
			log.error(e.getMessage(), e);
			// 记录异常信息
			ResponseConverter.setControllerException(controller, new DoNotRetryIOException(e));
		}
		finally
		{
			if (accumulator != null)
			{
				accumulator = null;
			}
			if (scanner != null)
			{
				try
				{
					scanner.close();
				}
				catch(Exception e)
				{
					log.error(e.getMessage(), e);
				}
			}
		}
		done.run(response);
	}

}
