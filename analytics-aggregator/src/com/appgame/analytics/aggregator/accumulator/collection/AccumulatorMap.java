package com.appgame.analytics.aggregator.accumulator.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccumulatorMap implements AccumulatorCollection
{
	/**
	 * 分组字段（分组条件）
	 */
	private List<String> kfields = new ArrayList<String>();
	
	/**
	 * 分组字段（分组内容）
	 */
	private List<String> vfields = new ArrayList<String>();
	
	/**
	 * 底层数据容器（按分组字段排序）
	 */
	private Map<List<Object>, List<Object>> container = new HashMap<List<Object>, List<Object>>();
	
	/**
	 * 构造方法（指定分组字段）
	 */
	public AccumulatorMap(Collection<String> kfields, Collection<String> vfields)
	{
		this.kfields.addAll(kfields);
		this.vfields.addAll(vfields);
	}
	
	/**
	 * 数组转字典（因为仅仅在聚合器内使用，所以不检查字段是否匹配）
	 */
	private Map<String, Object> convert(List<String> keys, List<Object> values)
	{
		Map<String, Object> result = new HashMap<String, Object>();
		for (int index = 0; index < keys.size(); index++)
		{
			result.put(keys.get(index), values.get(index));
		}
		return result;
	}
	
	/**
	 * 字典转数组（因为仅仅在聚合器内使用，所以不检查字段是否匹配）
	 */
	private List<Object> convert(List<String> keys, Map<String, Object> values)
	{
		List<Object> result = new ArrayList<Object>(keys.size());
		for (String key : keys)
		{
			result.add(values.get(key));
		}
		return result;
	}

	/**
	 * 构造分组条件(因为仅仅在聚合器内使用，所以不检查字段是否匹配)
	 */
	private List<Object> condition(Map<String, Object> values)
	{
		@SuppressWarnings("serial")
		List<Object> result = new ArrayList<Object>()
		{
			/**
			 * 哈希值
			 */
			private int hcode = 0;
			
			/**
			 * 指定哈希逻辑（确保快速匹配）
			 */
			public int hashCode()
			{
				if (hcode == 0)
				{
					for (Object v : this)
					{
						hcode = hcode * 37 + v.toString().hashCode();
					}
				}
				return hcode;
			}
		};
		for (int index = 0; index < kfields.size(); index++)
		{
			result.add(values.get(kfields.get(index)));
		}
		return result;
	}
	
	/**
	 * 添加记录到容器
	 */
	public void put(Map<String, Object> key, Map<String, Object> value)
	{
		container.put(condition(key), convert(vfields, value));
	}
	
	/**
	 * 从容器取出指定记录
	 */
	public Map<String, Object> get(Map<String, Object> key)
	{
		List<Object> value = container.get(condition(key));
		if (value != null)
		{
			return convert(vfields, value);
		}
		else
		{
			return new HashMap<String, Object>();
		}
	}
	
	/**
	 * 返回容器内元素数量
	 */
	@Override
	public int size()
	{
		return container.size();
	}
	
	/**
	 * 定义容器内元素的迭代器(因为仅仅在聚合器内使用，所以不检查字段是否匹配)
	 */
	private class AccumulatorIterator implements Iterator<Map<String, Object>>
	{
		/**
		 * 底层迭代器
		 */
		Iterator<List<Object>> iterator = container.keySet().iterator();
		
		/**
		 * 判断容器内是否还有数据
		 */
		@Override
		public boolean hasNext()
		{
			return iterator.hasNext();
		}

		/**
		 * 获取容器内下一个数据
		 */
		@Override
		public Map<String, Object> next()
		{
			Map<String, Object> result = new HashMap<String, Object>();
			List<Object>        keys   = iterator.next();
			List<Object>        values = container.get(keys);
			// 转化分组条件
			if (kfields.size() == 1 && kfields.contains("_id"))
			{
				result.put("_id", keys.get(0));
			}
			else
			{
				result.put("_id", convert(kfields, keys));
			}
			// 转换分组内容
			if (values != null)
			{
				result.putAll(convert(vfields, values));
			}
			return result;
		}
	}

	/**
	 * 获取迭代器对象
	 */
	@Override
	public Iterator<Map<String, Object>> iterator()
	{
		return new AccumulatorIterator();
	}

}
