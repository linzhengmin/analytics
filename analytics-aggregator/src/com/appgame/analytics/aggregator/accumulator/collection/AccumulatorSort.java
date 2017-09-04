package com.appgame.analytics.aggregator.accumulator.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AccumulatorSort implements AccumulatorCollection
{
	/**
	 * 映射字段集合（容器内记录将按字段顺序保存）
	 */
	private List<String> kfields = new ArrayList<String>();
	
	/**
	 * 映射记录集合（每条记录将按映射到字段集合）
	 */
	private Set<List<Object>> container = null;
	
	/**
	 * 数组转字典（因为仅仅在聚合器内使用，所以不检查字段是否匹配）
	 */
	private Map<String, Object> convert(List<Object> values)
	{
		Map<String, Object> result = new HashMap<String, Object>();
		for (int index = 0; index < kfields.size(); index++)
		{
			result.put(kfields.get(index), values.get(index));
		}
		return result;
	}
	
	/**
	 * 字典转数组（因为仅仅在聚合器内使用，所以不检查字段是否匹配）
	 */
	private List<Object> convert(Map<String, Object> values)
	{
		List<Object> result = new ArrayList<Object>(kfields.size());
		for (String key : kfields)
		{
			result.add(values.get(key));
		}
		return result;
	}
	
	/**
	 * 数据比较器
	 */
	private class AccumulatorComparator implements Comparator<List<Object>>
	{
		/**
		 * 底层数据比较器（构造时指定）
		 */
		Comparator<Map<String, Object>> comparator = null;
		
		/**
		 * 构造方法
		 */
		public AccumulatorComparator(Comparator<Map<String, Object>> comparator)
		{
			this.comparator = comparator;
		}
		
		/**
		 * 比较逻辑（格式转换后，调用底层数据比较器）
		 */
		@Override
		public int compare(List<Object> o1, List<Object> o2)
		{
			return comparator.compare(convert(o1), convert(o2));
		}
	}
	
	/**
	 * 构造方法（指定容器映射字段以及比较逻辑）
	 */
	public AccumulatorSort(final Collection<String> keys, final Comparator<Map<String, Object>> comparator)
	{
		// 记录映射字段
		this.kfields.addAll(keys);
		// 构造映射集合
		this.container = new TreeSet<List<Object>>(new AccumulatorComparator(comparator));
	}
	
	/**
	 * 添加记录到容器
	 */
	public void add(Map<String, Object> values)
	{
		container.add(convert(values));
	}

	/**
	 * 返回容器内保存的记录数量
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
		private Iterator<List<Object>> iterator = container.iterator();
		
		/**
		 * 判断容器内是否还有数据
		 */
		@Override
		public boolean hasNext()
		{
			return iterator.hasNext();
		}

		/**
		 * 获取容器内的下一个数据
		 */
		@Override
		public Map<String, Object> next()
		{
			Map<String, Object> result = new HashMap<String, Object>();
			List<Object>        values = iterator.next();
			for (int index = 0; index < kfields.size(); index++)
			{
				result.put(kfields.get(index), values.get(index));
			}
			return result;
		}
	}
	
	/**
	 * 返回迭代器
	 */
	@Override
	public Iterator<Map<String, Object>> iterator()
	{
		return new AccumulatorIterator();
	}

}
