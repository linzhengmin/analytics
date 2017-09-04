package com.appgame.analytics.aggregator.accumulator.collection;

import java.util.Map;

public interface AccumulatorCollection extends Iterable<Map<String, Object>>
{
	/**
	 * 返回容器内元素数量
	 * @return
	 */
	public int size();
}
