package com.appgame.analytics.aggregator.accumulator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorArray;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.appgame.analytics.aggregator.accumulator.expression.Expression;

public class ProjectAccumulator extends Accumulator
{	
	/**
	 * 修饰的累积器
	 */
	private Accumulator accumulator;

	/**
	 * 映射字段集（累积器将按这个结构执行映射操作）
	 */
	private Map<String, Expression> expressions = new HashMap<String, Expression>();
	
	/**
	 * 数据集合（作为最终累积器时使用）
	 */
	private AccumulatorArray values = null;
	
	/**
	 * 构造映射累积器
	 * @param accumulator
	 * @param json
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public ProjectAccumulator(Accumulator accumulator, JSONObject json) throws Exception
	{
		// 底层累积器对象
		this.accumulator = accumulator;
		
		// 构造映射表达式
		for (Iterator it = json.keys(); it.hasNext(); )
		{
			String key = (String)it.next();
			expressions.put(key, Expression.build(json.get(key)));
		}
		
		// 构造数据容器
		this.values = new AccumulatorArray(expressions.keySet());
	}
	
	/**
	 * 处理输入累积器的数据
	 */
	@Override
	public void put(Map<String, Object> input) throws Exception
	{
		Map<String, Object> map = new HashMap<String, Object>();
		for (Map.Entry<String, Expression> entry : expressions.entrySet())
		{ 
			map.put(entry.getKey(), entry.getValue().execute(input));
		}
		// 处理字段映射结果
		if (accumulator != null)
		{
			accumulator.put(map);
		}
		else
		{
			values.add(map);
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
			return values;
		}
	}
}
