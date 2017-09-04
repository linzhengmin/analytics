package com.appgame.analytics.aggregator.accumulator.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccumulatorArray implements AccumulatorCollection
{
	/**
	 * 映射字段集合（容器内记录将按字段顺序保存）
	 */
	private List<String> kfields = new ArrayList<String>();
	
	/**
	 * 映射记录集合（每条记录将按映射到字段集合）
	 */
	private List<Object[]> container = new ArrayList<Object[]>();
	
	/**
	 * 构造方法（指定容器映射字段）
	 */
	public AccumulatorArray(Collection<String> keys)
	{
		if (keys != null)
		{
			this.kfields.addAll(keys);
		}
	}
	
	/**
	 * 添加记录到容器(因为仅仅在聚合器内使用，所以不检查字段是否匹配)
	 */
	public void add(Map<String, Object> map)
	{
		Object[] values = new Object[kfields.size()];
		for (int index = 0; index < kfields.size(); index++)
		{
			values[index] = map.get(kfields.get(index));
		}
		container.add(values);
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
		private Iterator<Object[]> iterator = container.iterator();
		
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
			Object[]            values = iterator.next();
			for (int index = 0; index < kfields.size(); index++)
			{
				result.put(kfields.get(index), values[index]);
			}
			return result;
		}
		
		/**
		 * 删除最后访问数据
		 */
		@Override
		public void remove()
		{
			iterator.remove();
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
