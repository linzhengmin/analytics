package com.appgame.analytics.aggregator.utils;

public class MemoryChecker
{
	/**
	 * 默认预留可用内存空间(10% - 50%)
	 */
	private final static double MIN_RESERVE_RATIO = 0.1;
	private final static double MAX_RESERVE_RATIO = 0.5;

	/**
	 * 系统预留可用内存报警阈值
	 */
	private long reserve;
	
	/**
	 * 构造方法（按比例预留空间[10%, 50%]）
	 * @param ratio
	 */
	public MemoryChecker(double ratio)
	{
		if (ratio < MIN_RESERVE_RATIO)
		{
			ratio = MIN_RESERVE_RATIO;
		}
		if (ratio > MAX_RESERVE_RATIO)
		{
			ratio = MAX_RESERVE_RATIO;
		}
		this.reserve = Math.round(ratio * Runtime.getRuntime().maxMemory());
	}
	
	/**
	 * 默认构造方法
	 */
	public MemoryChecker()
	{
		this(MIN_RESERVE_RATIO);
	}
	
	/**
	 * 检测是否超过指定阈值
	 * @return
	 */
	public boolean exceed()
	{
		return Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) <= reserve;
	}
	
}
