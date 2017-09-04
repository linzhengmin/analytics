package com.appgame.analytics.aggregator.accumulator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorMap;
import com.appgame.analytics.aggregator.accumulator.expression.Expression;
import com.appgame.analytics.aggregator.accumulator.utils.AccumulatorUtils;

public class GroupAccumulator extends Accumulator
{
	/**
	 * 定义二元算子
	 */
	private interface BinaryOperator<T>
	{
		public T apply(T v1, T v2);
	}
	
	/**
	 * 分组映射的二元算子列表
	 */
	private static Map<String, BinaryOperator<Object>> operators = new HashMap<String, BinaryOperator<Object>>();
	static
	{
		operators.put("$first", new BinaryOperator<Object>()
		{
			@Override
			public Object apply(Object v1, Object v2)
			{
				return v1 == null ? v2 : v1;
			}
		});
		operators.put("$last", new BinaryOperator<Object>()
		{
			@Override
			public Object apply(Object v1, Object v2)
			{
				return v2 == null ? v1 : v2;
			}
		});
		operators.put("$max", new BinaryOperator<Object>()
		{
			@Override
			public Object apply(Object v1, Object v2)
			{
				return AccumulatorUtils.max(v1, v2);
			}
		});
		operators.put("$min", new BinaryOperator<Object>()
		{
			@Override
			public Object apply(Object v1, Object v2)
			{
				return v1 == null ? v2 : AccumulatorUtils.min(v1, v2);
			}
		});
		operators.put("$sum", new BinaryOperator<Object>()
		{
			@Override
			public Object apply(Object v1, Object v2)
			{
				double v = 0.0;
				if (v1 instanceof Number)
				{
					v = v + ((Number)v1).doubleValue();
				}
				if (v2 instanceof Number)
				{
					v = v + ((Number)v2).doubleValue();
				}
				return v;
			}
		});
		operators.put("$put", new BinaryOperator<Object>()
		{
			@SuppressWarnings("unchecked")
			@Override
			public Object apply(Object v1, Object v2)
			{
				List<Object> v = (List<Object>)v1;
				if (v == null)
				{
					v = new ArrayList<Object>();
				}
				if (v2 != null)
				{
					if (v2 instanceof Collection)
					{
						Collection<Object> c = (Collection<Object>)v2;
						for (Object o : c)
						{
							if (!v.contains(o))
							{
								v.add(o);
							}
						}
					}
					else
					{
						if (!v.contains(v2))
						{
							v.add(v2);
						}
					}
				}
				return v;
			}
		});
	}
	
	/**
	 * 数据分组策略
	 */
	private static class Separator
	{		
		/**
		 * 数据分组表达式集合
		 */
		private Map<String, Expression> expressions = new HashMap<String, Expression>();
		
		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("rawtypes")
		public Separator(Object input) throws Exception
		{
			if (input instanceof JSONObject)
			{
				JSONObject json = (JSONObject)input;
				Iterator   iter = json.keys();
				while(iter.hasNext())
				{
					String key = (String)iter.next();
					expressions.put(key, Expression.build(json.get(key)));
				}
			}
			else if (input instanceof String)
			{
				expressions.put("_id", Expression.build(input));
			}
			else if (org.codehaus.jettison.json.JSONObject.NULL.equals(input))
			{
				expressions.put("_id", Expression.build("null"));
			}
			if (expressions.isEmpty())
			{
				throw new Exception("{$group:{<_id>:<expression>, <field>:{<opname>:<expression>}, ...}} - '$group' must contain expression!");
			}
		}
		
		/**
		 * 获取数据分组索引
		 * @param input
		 * @return
		 * @throws Exception
		 */
		public Map<String, Object> apply(Map<String, Object> input) throws Exception
		{
			Map<String, Object> map = new HashMap<String, Object>(expressions.size());
			for (Map.Entry<String, Expression> entry : expressions.entrySet())
			{
				map.put(entry.getKey(), entry.getValue().execute(input));
			}
			return map;
		}
	}
	
	/**
	 * 数据投影策略
	 */
	private static class Projector
	{
		/**
		 * 数据投影表达式集合
		 */
		private Map<String, Pair<BinaryOperator<Object>, Expression>> expressions;

		/**
		 * 构造方法
		 */
		public Projector()
		{
			this.expressions = new HashMap<String, Pair<BinaryOperator<Object>, Expression>>();
		}

		/**
		 * 添加映射逻辑
		 * @param key
		 * @param json
		 * @throws Exception
		 */
		public void put(String key, JSONObject json) throws Exception
		{
			if (json.length() == 1)
			{
				String opname = (String)json.keys().next();
				if (operators.get(opname) != null)
				{
					expressions.put(key, new Pair<BinaryOperator<Object>, Expression>(operators.get(opname), Expression.build(json.get(opname))));
				}
				else
				{
					throw new Exception(String.format("{$group:{<_id>:<expression>, <field>:{<opname>:<expression>}, ...}} - unknown opname[%s]", opname));
				}
			}
			else
			{
				throw new Exception("{$group:{<_id>:{<field>:<expression>, ...}, <field>:{<opname>:<expression>}, ...}} - projective must contain only one operator");
			}
		}
		
		/**
		 * 计算分组投影结果
		 * @param m1
		 * @param m2
		 * @return
		 * @throws Exception
		 */
		public Map<String, Object> apply(Map<String, Object> m1, Map<String, Object> m2) throws Exception
		{
			Map<String, Object> values = new HashMap<String, Object>();
			for (Map.Entry<String, Pair<BinaryOperator<Object>, Expression>> entry : expressions.entrySet())
			{
				Object v1  = m1 == null ? null : m1.get(entry.getKey());
				Object v2  = entry.getValue().getSecond().execute(m2);
				values.put(entry.getKey(), entry.getValue().getFirst().apply(v1, v2));
			}
			return values;
		}
	}

	/**
	 * 修饰的累积器
	 */
	private Accumulator accumulator;
	
	/**
	 * 分组策略
	 */
	private Separator separator = null;

	/**
	 * 映射逻辑
	 */
	private Projector projector = null;
	
	/**
	 * 分组数据集合<关键字， 分组字段>
	 */
	private AccumulatorMap collection = null;
	
	/**
	 * 构造分组累积器
	 * @param accumulator
	 * @param json
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public GroupAccumulator(Accumulator accumulator, JSONObject json) throws Exception
	{
		// 底层累积器对象
		this.accumulator = accumulator;
		
		// 映射配置器(默认为不映射任何数据)
		projector = new Projector();
		
		// 遍历累积器配置项，构造分组脚本以及映射脚本
		for (Iterator it = json.keys(); it.hasNext(); )
		{
			String key = (String)it.next();
			if (key.equals("_id"))
			{
				separator = new Separator(json.get(key));
			}
			else
			{
				projector.put(key, json.getJSONObject(key));
			}
		}
		if (separator == null)
		{
			throw new Exception("{$group:{<_id>:{<field>:<expression>, ...}, <field>:{<opname>:<expression>}, ...}} - must specify the '_id' section");
		}

		// 构造分组数据集
		this.collection = new AccumulatorMap(separator.expressions.keySet(), projector.expressions.keySet());
	}
	
	/**
	 * 处理输入累积器的数据
	 */
	@Override
	public void put(Map<String, Object> input) throws Exception
	{
		// 计算输入数据的分组索引
		Map<String, Object> key = separator.apply(input);
		// 按分组索引处理输入数据
		if (projector != null)
		{
			collection.put(key, projector.apply(collection.get(key), input));
		}
		else
		{
			collection.put(key, null);
		}
	}

	/**
	 * 从累积器读取数据
	 */
	@Override
	public AccumulatorCollection get() throws Exception
	{
		if (accumulator != null)
		{
			for (Iterator<Map<String, Object>> iterator = collection.iterator(); iterator.hasNext(); )
			{
				accumulator.put(iterator.next());
			}
			return accumulator.get();
		}
		else
		{
			return collection;
		}
	}
}
