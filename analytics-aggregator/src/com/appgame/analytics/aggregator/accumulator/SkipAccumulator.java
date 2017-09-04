package com.appgame.analytics.aggregator.accumulator;

import java.util.ArrayList;
import java.util.Map;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorArray;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;

public class SkipAccumulator extends Accumulator
{
	/**
	 * 修饰的累积器
	 */
	private Accumulator accumulator;
	
	/**
	 * 需要跳过的数据数量
	 */
	private long skip = 0;
	
	/**
	 * 数据集合（作为最终累积器时使用）
	 */
	private AccumulatorArray values;

	/**
	 * 构造跳过累积器
	 * @param accumulator
	 * @param skip
	 */
	public SkipAccumulator(Accumulator accumulator, long skip)
	{
		this.accumulator = accumulator;
		this.skip        = skip;
		this.values      = null;
	}
	
	/**
	 * 处理输入累积器的数据
	 */
	@Override
	public void put(Map<String, Object> input) throws Exception
	{
		if (skip <= 0)
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
		else
		{
			skip = skip - 1;
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
