package com.appgame.analytics.aggregator.accumulator.utils;

public class AccumulatorUtils
{
	/**
	 * 基础对象比较逻辑
	 * @param v1
	 * @param v2
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int compare(Object v1, Object v2)
	{
		if (v1 == null || v2 == null)
		{
			return (v1 == null) ? ((v2 == null) ? 0 : -1) : 1;
		}
		else
		{
			if (v1.getClass() == v2.getClass())
			{
				if (v1 instanceof Comparable)
				{
					return ((Comparable) v1).compareTo(v2);
				}
				else
				{
					return v1.toString().compareTo(v2.toString());
				}
			}
			else
			{
				if ((v1 instanceof Number) && (v2 instanceof Number))
				{
					return Double.compare(((Number)v1).doubleValue(), ((Number)v2).doubleValue());
				}
				else
				{
					return v1.getClass().getName().compareTo(v2.getClass().getName());
				}
			}
		}
	}
	
	/**
	 * 从指定对象中获取较大对象
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static Object max(Object v1, Object v2)
	{
		return (compare(v1, v2) >= 0) ? v1 : v2;
	}
	
	/**
	 * 从指定对象中获取较小对象
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static Object min(Object v1, Object v2)
	{
		return (compare(v1, v2) <= 0) ? v1 : v2;
	}
	
	/**
	 * 基础对象转为布尔对象
	 * @param v
	 * @return
	 */
	public static boolean bool(Object v)
	{
		if (v == null)
		{
			return false;
		}
		else if (v instanceof Boolean)
		{
			return (Boolean)v;
		}
		else if (v instanceof Number)
		{			
			return (((Number)v).doubleValue() != 0.0);
		}
		else
		{
			return true;
		}
	}
	
}
