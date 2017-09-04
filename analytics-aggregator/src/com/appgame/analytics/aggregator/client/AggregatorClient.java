package com.appgame.analytics.aggregator.client;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.xerial.snappy.SnappyInputStream;

import com.appgame.analytics.aggregator.accumulator.Accumulator;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.AggregateResponse;
import com.appgame.analytics.aggregator.protobuf.generated.AggregateProtos.AggregateService;
import com.appgame.analytics.aggregator.utils.AggregatorUtils;
import com.appgame.analytics.aggregator.utils.ObjectInputStream;

public class AggregatorClient implements Closeable
{
	/**
	 * 日志记录器
	 */
	private static Logger log = Logger.getLogger(AggregatorClient.class);

	/**
	 * 聚合操作超时配置项
	 */
	private static String AGGREGATE_TIMEOUT_KEY   = "hbase.thrift.aggregate.timeout.ms";
	private static long   AGGREGATE_TIMEOUT_VALUE = 120000;
	
	/**
	 * 连接器
	 */
	private Connection connection = null;
	
	/**
	 * 聚合器操作相关配置项
	 */
	private Configuration config = null;
	
	/**
	 * 构造方法
	 * @param config
	 */
	public AggregatorClient(final Configuration config)
	{
		try
		{
			this.config     = config;
			this.connection = ConnectionFactory.createConnection(config);
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 构造方法（指定连接用户）
	 * @param config
	 * @param uname
	 */
	public AggregatorClient(final Configuration config, final String uname)
	{
		try
		{
			this.config     = config;
			this.connection = User.create(UserGroupInformation.createRemoteUser(uname)).runAs(new PrivilegedExceptionAction<Connection>()
			{
				public Connection run() throws Exception
				{
					return ConnectionFactory.createConnection(config);
				}
			});
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void close() throws IOException
	{
		if (connection != null && !connection.isClosed())
		{
			connection.close();
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * 服务端回调类型
	 */
	private class AggregateCallback implements Batch.Callback<AggregateResponse>
	{
		/**
		 * 客户端累积器
		 */
		private Accumulator accumulator = null;
		
		/**
		 * 构造方法（指定客户端累积器）
		 * @param client
		 * @throws Exception
		 */
		public AggregateCallback(String client) throws Exception
		{
			this.accumulator = Accumulator.build(client);
		}
		
		/**
		 * 获得聚合结果
		 * @return
		 * @throws Exception
		 */
		public AccumulatorCollection get() throws Exception
		{
			return accumulator.get();
		}

		/**
		 * RegionService 返回数据处理
		 * @param region
		 * @param row
		 * @param result
		 */
		@SuppressWarnings("unchecked")
		@Override
		public synchronized void update(byte[] region, byte[] row, AggregateResponse response)
		{
			//　数据转换（必须与服务端数据序列化逻辑相匹配）
			try
			{
				ByteArrayInputStream bis = new ByteArrayInputStream(response.getData().toByteArray());
				SnappyInputStream    sis = new SnappyInputStream(bis);
				ObjectInputStream    ois = new ObjectInputStream(sis);
				int size = ois.readInt();
				for (int i = 0; i < size; i++)
				{
					accumulator.put((Map<String, Object>)ois.readObject());
				}
				ois.close();
			}
			catch (Exception e)
			{
				log.error(e.getMessage(), e);
			}
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * 聚合操作的同步锁管理器(提供公共聚合器实例对象定位同步锁的操作接口)
	 */
	private class AggregateLock
	{
		/**
		 * 锁集合(每个RS服务器对应一个锁对象)
		 */
		private Map<String, Object> locks;
		
		/**
		 * 构造方法
		 */
		public AggregateLock()
		{
			locks = new HashMap<String, Object>();
		}
		
		/**
		 * 返回指定对象的指定属性(同时将指定属性设置为可访问)
		 * @param instance
		 * @param name
		 * @return
		 * @throws SecurityException 
		 * @throws NoSuchFieldException 
		 * @throws IllegalAccessException 
		 * @throws IllegalArgumentException 
		 */
		private Object attribute(Object instance, String name) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
		{
			Field field = instance.getClass().getDeclaredField(name);
			field.setAccessible(true);
			return field.get(instance);
		}

		/**
		 * 返回聚合操作实例对应的锁对象
		 * @param instance
		 * @return
		 */
		public Object location(AggregateService instance)
		{
			// 获取聚合操作的目标服务器名称
			String server = "null";
			try
			{
				Object channel    = attribute(instance, "channel"   );
				Object connection = attribute(channel , "connection");
				Object table      = attribute(channel , "table"     );
				Object row        = attribute(channel , "row"       );
				server = ((Connection)connection).getRegionLocator((TableName)table).getRegionLocation((byte[])row).getServerName().getServerName();
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException | IOException e)
			{
				server = "null";
				log.error(e.getMessage(), e);
			}
			
			// 根据服务器名称获取聚合操作对应锁对象
			synchronized(this)
			{
				Object lock = locks.get(server);
				if (lock == null)
				{
					lock = new Object();
					locks.put(server, lock);
				}
				return lock;
			}
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * 聚合操作（带RS端聚合命令）
	 * @param table
	 * @param scans
	 * @param client
	 * @param server
	 * @return
	 * @throws Throwable
	 */
	private AccumulatorCollection aggregate(Table table, List<Scan> scans, String client, String server) throws Throwable
	{
		// 构造客户端聚合回调
		AggregateCallback callback = new AggregateCallback(client);
		
		// 聚合操作同步锁管理对象
		final AggregateLock lock = new AggregateLock();
		
		// 构造服务端聚合请求
		final AggregateProtos.AggregateRequest.Builder builder = AggregateProtos.AggregateRequest.newBuilder();
		for (Scan scan : scans)
		{
			builder.addScans(AggregatorUtils.scan(scan));
		}
		builder.setPipes(server);
		
		// 聚合操作计时
		final long timeout = config.getLong(AGGREGATE_TIMEOUT_KEY, AGGREGATE_TIMEOUT_VALUE);
		final long origin  = System.currentTimeMillis();
		// 执行聚合操作
		table.coprocessorService(AggregateService.class, null, null, new Batch.Call<AggregateService, AggregateResponse>()
		{
			@Override
			public AggregateResponse call(AggregateService instance) throws IOException
			{
				// RPC控制器
				ServerRpcController controller = new ServerRpcController();
				// RPC回调方法
				BlockingRpcCallback<AggregateResponse> rcb = new BlockingRpcCallback<AggregateResponse>();
				
				// 定位同步锁并锁定聚合操作
				synchronized(lock.location(instance))
				{
					// 超时检查(伪,仅仅判断线程开始工作时是否超时)
					if (System.currentTimeMillis() - origin >= timeout)
					{
						throw new IOException("aggregate timeout!!");
					}
					else
					{
						long start = System.currentTimeMillis();
						log.info(String.format("----aggregate[%d] - start", Thread.currentThread().getId()));
						instance.aggregate(controller, builder.build(), rcb);
						log.info(String.format("----aggregate[%d] - over = %d", Thread.currentThread().getId(), System.currentTimeMillis() - start));
					}
				}
				// 处理返回结果
				if (controller.failedOnException())
				{
					throw controller.getFailedOn();
				}
				else
				{
					try
					{
						return rcb.get();
					}
					catch (Exception e)
					{
						throw new IOException(e);
					}
				}
			}
		}, callback);
		return callback.get();
	}
	
	/**
	 * 聚合操作入口
	 * @param tablename
	 * @param scans
	 * @param commands
	 * @return
	 * @throws Throwable
	 */
	public AccumulatorCollection aggregate(String tablename, List<Scan> scans, String commands) throws Throwable
	{
		JSONObject json    = new JSONObject(commands);
		JSONArray  jclient = json.getJSONArray("$client");
		JSONArray  jserver = json.getJSONArray("$server");
		if (jclient == null)
		{
			throw new Exception("{ { $server : [{<stage>}, ...] }, { $client : [{<stage>}, ...] } } - must contain [$client] section!!");
		}
		if (jserver == null)
		{
			throw new Exception("{ { $server : [{<stage>}, ...] }, { $client : [{<stage>}, ...] } } - must contain [$server] section!!");
		}
		return aggregate(connection.getTable(TableName.valueOf(tablename)), scans, jclient.toString(), jserver.toString());
	}

}
