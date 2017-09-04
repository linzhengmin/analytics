package com.appgame.analytics.aggregator.accumulator;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.google.common.collect.LinkedListMultimap;

public abstract class Accumulator
{
	/////////////////////////////////////////////////////////////////
	// 累积器接口定义
	/////////////////////////////////////////////////////////////////
	
	/**
	 * 将数据压入累积器
	 * @param input
	 * @throws Exception
	 */
	public abstract void put(Map<String, Object> input) throws Exception;
	
	/**
	 * 从累积器读取数据
	 * @return
	 * @throws Exception
	 */
	public abstract AccumulatorCollection get() throws Exception;
	
	/////////////////////////////////////////////////////////////////
	// 异常捕捉累积器
	/////////////////////////////////////////////////////////////////
	
	private static class WrapperAccumulator extends Accumulator
	{
		/**
		 * 累积器描述语句
		 */
		private String sentence = "";
		
		/**
		 * 底层累积器对象
		 */
		private Accumulator accumulator;
		
		/**
		 * 构造方法
		 * @param sentence
		 * @param accumulator
		 */
		public WrapperAccumulator(String sentence, Accumulator accumulator)
		{
			this.sentence = sentence;
			if (accumulator != null)
			{
				this.accumulator = accumulator;
			}
			else
			{
				String.format("accumulator must contain exactly one stage - %s", sentence);
			}
		}
		
		/**
		 * 处理输入累积器的数据
		 */
		@Override
		public void put(Map<String, Object> input) throws Exception
		{
			try
			{
				accumulator.put(input);
			}
			catch (Exception e)
			{
				StringBuilder builder = new StringBuilder("\r\n");
				builder.append("sentence : \r\n");
				builder.append(sentence + "\r\n");
				builder.append("error : \r\n");
				builder.append(e.getMessage());
				builder.append("\r\n");
				throw new Exception(builder.toString(), e);
			}
		}

		/**
		 * 从累积器读取数据
		 */
		@Override
		public AccumulatorCollection get() throws Exception
		{
			return accumulator.get();
		}
	}
	
	
	/////////////////////////////////////////////////////////////////
	// 累积器构造方法
	/////////////////////////////////////////////////////////////////
	
	/**
	 * 定义构造器
	 */
	private interface AccumulatorBuilder
	{
		public Accumulator build(Accumulator accumulator, Object value) throws Exception;
	}
	
	/**
	 * 构造器映射表
	 */
	private static Map<String, AccumulatorBuilder> builders = new HashMap<String, AccumulatorBuilder>();
	static
	{
		builders.put("$project", new AccumulatorBuilder()
		{
			@Override
			public Accumulator build(Accumulator accumulator, Object value)	throws Exception
			{
				if (value instanceof JSONObject)
				{
					return new ProjectAccumulator(accumulator, (JSONObject)value);
				}
				else
				{
					throw new Exception("$project:{<specifications>} - invalid parameters");
				}
			}
		});
		builders.put("$group", new AccumulatorBuilder()
		{
			@Override
			public Accumulator build(Accumulator accumulator, Object value)	throws Exception
			{
				if (value instanceof JSONObject)
				{
					return new GroupAccumulator(accumulator, (JSONObject)value);
				}
				else
				{
					throw new Exception("$group:{<specifications>} - invalid parameters");
				}
			}
		});
		builders.put("$sort", new AccumulatorBuilder()
		{
			@Override
			public Accumulator build(Accumulator accumulator, Object value)	throws Exception
			{
				if (value instanceof JSONObject)
				{
					return new SortAccumulator(accumulator, (JSONObject)value);
				}
				else
				{
					throw new Exception("$sort:{<specifications>} - invalid parameters");
				}
			}
		});
		builders.put("$skip", new AccumulatorBuilder()
		{
			@Override
			public Accumulator build(Accumulator accumulator, Object value)	throws Exception
			{
				if (value instanceof Number)
				{
					return new SkipAccumulator(accumulator, ((Number) value).longValue());
				}
				else
				{
					throw new Exception("$skip:<positive integer> - invalid parameters");
				}
			}
		});
		builders.put("$limit", new AccumulatorBuilder()
		{
			@Override
			public Accumulator build(Accumulator accumulator, Object value)	throws Exception
			{
				if (value instanceof Number)
				{
					return new LimitAccumulator(accumulator, ((Number) value).longValue());
				}
				else
				{
					throw new Exception("$limit:<positive integer> - invalid parameters");
				}
			}
		});
		builders.put("$match", new AccumulatorBuilder()
		{
			@Override
			public Accumulator build(Accumulator accumulator, Object value)	throws Exception
			{
				if (value instanceof JSONObject)
				{
					return new MatchAccumulator(accumulator, (JSONObject)value);
				}
				else
				{
					throw new Exception("$match:{ <field> : {<condition>}, ....} - invalid parameters");
				}
			}
		});
	}
	
	/**
	 * 构造累积器
	 * @param sentence
	 * @return
	 * @throws Exception
	 */
	public static Accumulator build(String sentence) throws Exception
	{
		JSONArray jarray = new JSONArray(sentence);
		// 累积器配置解析
		LinkedListMultimap<String, Object> map = LinkedListMultimap.create();
		for (int i = 1; i <= jarray.length(); ++i)
		{
			JSONObject json = jarray.getJSONObject(jarray.length() - i);
			if (json.length() == 1)
			{
				String key = (String)json.keys().next();
				map.put(key, json.get(key));
			}
			else
			{
				throw new Exception(String.format("accumulator must contain exactly one field - %s", json.toString()));
			}
		}
		// 构造累积器
		Accumulator accumulator = null;
		for (Map.Entry<String, Object> entry : map.entries())
		{
			AccumulatorBuilder builder = builders.get(entry.getKey());
			if (builder != null)
			{
				accumulator = builder.build(accumulator, entry.getValue());
			}
			else
			{
				throw new Exception(String.format("unknown accumulator[%s]", entry.getKey()));
			}
		}
		return new WrapperAccumulator(sentence, accumulator);
	}

}
