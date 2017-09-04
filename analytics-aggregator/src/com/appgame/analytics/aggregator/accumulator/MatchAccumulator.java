package com.appgame.analytics.aggregator.accumulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorArray;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.appgame.analytics.aggregator.accumulator.expression.Condition;

public class MatchAccumulator extends Accumulator
{
	/**
	 * 定义条件算子
	 */
	private static abstract class Operator
	{
		/**
		 * 同一分组下的判断条件
		 */
		private Map<String, Condition> conditions = new HashMap<String, Condition>();
		
		/**
		 * 条件执行方法（抽象接口）
		 * @param input
		 * @return
		 * @throws Exception
		 */
		public abstract boolean apply(Map<String, Object> input) throws Exception;
		
		/**
		 * 增加判断条件
		 * @param key
		 * @param condition
		 */
		public void put(String key, Condition condition)
		{
			conditions.put(key, condition);
		}
	}
	
	/**
	 * 条件算子构造器
	 * @param key
	 * @param value
	 * @return
	 * @throws Exception 
	 */
	@SuppressWarnings("rawtypes")
	private static Operator build(String type, Object value) throws Exception
	{
		/**
		 * 按类型构造条件算子
		 */
		Operator operator = null;
		if (type.equals("$or"))
		{
			operator = new Operator()
			{
				@Override
				public boolean apply(Map<String, Object> input) throws Exception
				{
					for (Map.Entry<String, Condition> entry : super.conditions.entrySet())
					{
						Condition condition = entry.getValue();
						if (condition.execute(input.get(entry.getKey())))
						{
							return true;
						}
					}
					return false;
				}
			};
		}
		else if (type.equals("$nor"))
		{
			operator = new Operator()
			{
				@Override
				public boolean apply(Map<String, Object> input) throws Exception
				{
					for (Map.Entry<String, Condition> entry : super.conditions.entrySet())
					{
						Condition condition = entry.getValue();
						if (condition.execute(input.get(entry.getKey())))
						{
							return false;
						}
					}
					return true;
				}
			};
		}
		else if (type.equals("$not"))
		{
			operator = new Operator()
			{
				@Override
				public boolean apply(Map<String, Object> input) throws Exception
				{
					for (Map.Entry<String, Condition> entry : super.conditions.entrySet())
					{
						Condition condition = entry.getValue();
						if (!condition.execute(input.get(entry.getKey())))
						{
							return true;
						}
					}
					return false;
				}
			};
		}
		else
		{
			// 默认使用 "$and" 条件算子
			operator = new Operator()
			{
				@Override
				public boolean apply(Map<String, Object> input) throws Exception
				{
					for (Map.Entry<String, Condition> entry : super.conditions.entrySet())
					{
						Condition condition = entry.getValue();
						if (!condition.execute(input.get(entry.getKey())))
						{
							return false;
						}
					}
					return true;
				}
			};
		}
		
		/**
		 * 解析判断配置
		 */
		JSONObject json = null;
		if (type.equals("$and") || type.equals("$or") || type.equals("$nor") || type.equals("$not"))
		{
			json = (JSONObject)value;
		}
		else
		{
			json = new JSONObject().put(type, value);
		}
		
		/**
		 * 添加判断条件到条件算子内
		 */
		for (Iterator it = json.keys(); it.hasNext(); )
		{
			String key = (String)it.next();
			operator.put(key, Condition.build(json.get(key)));
		}
		return operator;
	}

	/**
	 * 修饰的累积器
	 */
	private Accumulator accumulator;
	
	/**
	 * 数据集合（作为最终累积器时使用）
	 */
	private AccumulatorArray values;
	
	/**
	 * 条件算子集合
	 */
	private List<Operator> operators = new ArrayList<Operator>();
	
	/**
	 * 构造匹配累积器
	 * @param accumulator
	 * @param json
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public MatchAccumulator(Accumulator accumulator, JSONObject json) throws Exception
	{
		// 底层累积器对象
		this.accumulator = accumulator;

		// 遍历匹配配置项， 构造匹配累积器
		for (Iterator it = json.keys(); it.hasNext(); )
		{
			String key = (String)it.next();
			operators.add(build(key, json.get(key)));
		}
	}
	
	/**
	 * 处理输入累积器的数据
	 */
	@Override
	public void put(Map<String, Object> input) throws Exception
	{
		boolean match = true;
		for (Operator operator : operators)
		{
			match = match & operator.apply(input);
			if (!match)
			{
				break;
			}
		}
		if (match)
		{
			if (accumulator != null)
			{
				accumulator.put(input);
			}
			else
			{
				if (values == null)
				{
					values = new AccumulatorArray(input.keySet());
				}
				values.add(input);
			}
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
			return accumulator.get();
		}
		else
		{
			return values != null ? values : new AccumulatorArray(new ArrayList<String>());
		}
	}
}
