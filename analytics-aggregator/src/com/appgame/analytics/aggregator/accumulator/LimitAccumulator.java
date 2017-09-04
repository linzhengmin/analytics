package com.appgame.analytics.aggregator.accumulator;

import java.util.ArrayList;
import java.util.Map;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorArray;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;

public class LimitAccumulator extends Accumulator
{
	/**
	 * 修饰的累积器
	 */
	private Accumulator accumulator;
	
	/**
	 * 可通过数据阀值
	 */
	private long limit = 0;
	
	/**
	 * 通过的数据数量
	 */
	private long count = 0;
	
	/**
	 * 数据集合（作为最终累积器时使用）
	 */
	private AccumulatorArray values;
	
	/**
	 * 构造限制累积器
	 * @param accumulator
	 * @param skip
	 */
	public LimitAccumulator(Accumulator accumulator, long limit)
	{
		this.accumulator = accumulator;
		this.limit       = limit;
		this.count       = 0;
		this.values      = null;
	}	
	
	/**
	 * 处理输入累积器的数据
	 */
	@Override
	public void put(Map<String, Object> input) throws Exception
	{
		if (count < limit)
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
			count = count + 1;
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
