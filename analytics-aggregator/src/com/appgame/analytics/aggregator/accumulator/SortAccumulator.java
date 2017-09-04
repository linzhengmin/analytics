package com.appgame.analytics.aggregator.accumulator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorCollection;
import com.appgame.analytics.aggregator.accumulator.collection.AccumulatorSort;
import com.appgame.analytics.aggregator.accumulator.utils.AccumulatorUtils;

public class SortAccumulator extends Accumulator
{
	/**
	 * 数据比较器
	 */
	private class AccumulatorComparetor implements Comparator<Map<String, Object>>
	{
		/**
		 * 比较字段集
		 */
		private Map<String, Integer> map = new HashMap<String, Integer>();
		
		/**
		 * 构造比较器
		 * @param json
		 * @throws Exception 
		 */
		@SuppressWarnings("rawtypes")
		public AccumulatorComparetor(JSONObject json) throws Exception
		{
			// 比较字段解析
			for (Iterator it = json.keys(); it.hasNext(); )
			{
				String key = (String)it.next();
				map.put(key, json.getInt(key) > 0 ? 1 : -1);
			}
			// 比较字段检查
			if (map.size() == 0)
			{
				throw new Exception("{$sort:{<field>:<order>, ... }} - must contain one or more fields");
			}
		}
		
		/**
		 * 比较逻辑（如果对象无法比较，则按输入顺序排列）
		 */
		@Override
		public int compare(Map<String, Object> m1, Map<String, Object> m2)
		{
			for (Map.Entry<String, Integer> entry : map.entrySet())
			{
				int ret = AccumulatorUtils.compare(m1.get(entry.getKey()), m2.get(entry.getKey()));
				if (ret > 0)
				{
					return entry.getValue();
				}
				else if (ret < 0)
				{
					return -entry.getValue();
				}
			}
			return -1;
		}
	}
	
	/**
	 * 修饰的累积器
	 */
	private Accumulator accumulator;
	
	/**
	 * 支持排序的数据集合
	 */
	private AccumulatorSort values;
	
	/**
	 * 内部数据比较器
	 */
	private Comparator<Map<String, Object>> comparator;
	
	/**
	 * 构造排序累积器
	 * @param accumulator
	 * @param json
	 * @throws Exception
	 */
	public SortAccumulator(Accumulator accumulator, JSONObject json) throws Exception
	{
		this.accumulator = accumulator;
		this.comparator  = new AccumulatorComparetor(json);
	}
	
	/**
	 * 处理输入累积器的数据
	 */
	@Override
	public void put(Map<String, Object> input) throws Exception
	{
		if (values == null)
		{
			values = new AccumulatorSort(input.keySet(), comparator);
		}
		values.add(input);
	}
	
	/**
	 * 从累积器读取数据
	 */
	@Override
	public AccumulatorCollection get() throws Exception
	{
		if (values != null)
		{
			if (accumulator != null)
			{
				for (Iterator<Map<String, Object>> iterator = values.iterator(); iterator.hasNext(); )
				{
					accumulator.put(iterator.next());
				}
				return accumulator.get();
			}
			else
			{
				return values;
			}
		}
		else
		{
			return new AccumulatorSort(new ArrayList<String>(), comparator);
		}
	}
}
