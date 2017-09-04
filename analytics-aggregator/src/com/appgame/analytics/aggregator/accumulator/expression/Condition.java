package com.appgame.analytics.aggregator.accumulator.expression;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.utils.AccumulatorUtils;

public class Condition
{
	/**
	 * 条件运算符标识
	 */
	@Retention(RetentionPolicy.RUNTIME)
	private @interface ConditionAnnotation
	{
	}
	
	/**
	 * 条件运算符接口
	 */
	private interface Operator
	{
		public boolean execute(Object input) throws Exception;
	}
	
	/**
	 * 条件对象关联运算符
	 */
	private Operator operator = null;
	
	/**
	 * 条件对象构造方法
	 * @param operator
	 */
	private Condition(Operator operator)
	{
		this.operator = operator;
	}
	
	/**
	 * 条件表达式执行方法
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public boolean execute(Object input) throws Exception
	{
		return operator.execute(input);
	}

	/**
	 * 按名称构建条件运算符
	 * @param key
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Operator build(String key, Object obj) throws Exception
	{
		if (!key.isEmpty() && key.charAt(0) == '$')
		{
			String opname = key.substring(1, key.length());
			for (Class clazz : Condition.class.getDeclaredClasses())
			{
				if (clazz.getSimpleName().equals(opname) && clazz.getAnnotation(ConditionAnnotation.class) != null)
				{
					return (Operator)(clazz.getConstructor(new Class[]{Object.class}).newInstance(obj));
				}
			}
		}
		throw new Exception("condition - unknown opname[" + key + "]");
	}
	
	/**
	 * 构造条件对象
	 * @param input
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static Condition build(Object input) throws Exception
	{
		// 格式化配置参数
		JSONObject json = null;
		if (input instanceof JSONObject)
		{
			json = (JSONObject)input;
		}
		else
		{
			json = new JSONObject().put("$eq", input);
		}
		
		// 按配置构造条件对象
		if (json.length() != 0)
		{
			if (json.length() == 1)
			{
				String key = (String)json.keys().next();
				Object obj = json.get(key);
				return new Condition(build(key, obj));
			}
			else
			{
				JSONArray jarray = new JSONArray();
				for (Iterator it = json.keys(); it.hasNext(); )
				{
					String key = (String)it.next();
					jarray.put(new JSONObject().put(key, json.get(key)));
				}
				return new Condition(new and(jarray));
			}
		}
		else
		{
			throw new Exception("condition - empty operator");
		}
	}
	
	
	/////////////////////////////////////////////////////////////////
	//
	// 布尔逻辑
	//
	/////////////////////////////////////////////////////////////////
	
	@ConditionAnnotation
	private static class and implements Operator
	{
		/**
		 * 条件表达式集
		 */
		private List<Operator> operators = new ArrayList<Operator>();
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("rawtypes")
		public and(Object input) throws Exception
		{
			JSONArray jarray = (JSONArray)input;
			for (int i = 0; i < jarray.length(); ++i)
			{
				Object obj = jarray.get(i);
				if (obj instanceof JSONObject)
				{
					JSONObject json = (JSONObject)obj;
					for (Iterator it = json.keys(); it.hasNext(); )
					{
						String key = (String)it.next();
						operators.add(build(key, json.get(key)));
					}
				}
				else
				{
					operators.add(new eq(obj));
				}
			}
			if (operators.isEmpty())
			{
				throw new Exception("condition - {$and : [<condition>, ....]} - need to provide one or more parameters");
			}
		}

		/**
		 * 执行条件判断
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			for (Operator operator : operators)
			{
				if (!operator.execute(input))
				{
					return false;
				}
			}
			return true;
		}
	}
	
	@ConditionAnnotation
	private static class or implements Operator
	{
		/**
		 * 条件表达式集
		 */
		private List<Operator> operators = new ArrayList<Operator>();
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings({ "rawtypes", "unused" })
		public or(Object input) throws Exception
		{
			JSONArray jarray = (JSONArray)input;
			for (int i = 0; i < jarray.length(); ++i)
			{
				Object obj = jarray.get(i);
				if (obj instanceof JSONObject)
				{
					JSONObject json = (JSONObject)obj;
					for (Iterator it = json.keys(); it.hasNext(); )
					{
						String key = (String)it.next();
						operators.add(build(key, json.get(key)));
					}
				}
				else
				{
					operators.add(new eq(obj));
				}
			}
			if (operators.isEmpty())
			{
				throw new Exception("condition - {$or : [<condition>, ....]} - need to provide one or more parameters");
			}
		}

		/**
		 * 执行条件判断
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			for (Operator operator : operators)
			{
				if (operator.execute(input))
				{
					return true;
				}
			}
			return false;
		}
	}
	
	@ConditionAnnotation
	private static class nor implements Operator
	{
		/**
		 * 条件表达式集
		 */
		private List<Operator> operators = new ArrayList<Operator>();
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings({ "rawtypes", "unused" })
		public nor(Object input) throws Exception
		{
			JSONArray jarray = (JSONArray)input;
			for (int i = 0; i < jarray.length(); ++i)
			{
				Object obj = jarray.get(i);
				if (obj instanceof JSONObject)
				{
					JSONObject json = (JSONObject)obj;
					for (Iterator it = json.keys(); it.hasNext(); )
					{
						String key = (String)it.next();
						operators.add(build(key, json.get(key)));
					}
				}
				else
				{
					operators.add(new eq(obj));
				}
			}
			if (operators.isEmpty())
			{
				throw new Exception("condition - {$nor : [<condition>, ....]} - need to provide one or more parameters");
			}
		}

		/**
		 * 执行条件判断
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			for (Operator operator : operators)
			{
				if (operator.execute(input))
				{
					return false;
				}
			}
			return true;
		}
	}
	
	@ConditionAnnotation
	private static class not implements Operator
	{
		/**
		 * 条件表达式集
		 */
		private List<Operator> operators = new ArrayList<Operator>();
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings({ "rawtypes", "unused" })
		public not(Object input) throws Exception
		{
			JSONArray jarray = (JSONArray)input;
			for (int i = 0; i < jarray.length(); ++i)
			{
				Object obj = jarray.get(i);
				if (obj instanceof JSONObject)
				{
					JSONObject json = (JSONObject)obj;
					for (Iterator it = json.keys(); it.hasNext(); )
					{
						String key = (String)it.next();
						operators.add(build(key, json.get(key)));
					}
				}
				else
				{
					operators.add(new eq(obj));
				}
			}
			if (operators.isEmpty())
			{
				throw new Exception("condition - {$not : [<condition>, ....]} - need to provide one or more parameters");
			}
		}

		/**
		 * 执行条件判断
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			for (Operator operator : operators)
			{
				if (!operator.execute(input))
				{
					return true;
				}
			}
			return false;
		}
	}
	
	
	/////////////////////////////////////////////////////////////////
	//
	// 比较逻辑
	//
	/////////////////////////////////////////////////////////////////
	
	@ConditionAnnotation
	private static class eq implements Operator
	{
		/**
		 * 比较对象
		 */
		private Object value;
		/**
		 * 构造方法
		 * @param value
		 */
		public eq(Object value) throws Exception
		{
			this.value = value;
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return (0 == AccumulatorUtils.compare(input, value));
		}
	}
	
	@ConditionAnnotation
	private static class ne implements Operator
	{
		/**
		 * 比较对象
		 */
		private Object value;
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public ne(Object value) throws Exception
		{
			this.value = value;
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return (0 != AccumulatorUtils.compare(input, value));
		}
	}
	
	@ConditionAnnotation
	private static class gt implements Operator
	{
		/**
		 * 比较对象
		 */
		private Object value;
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public gt(Object value) throws Exception
		{
			this.value = value;
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return (AccumulatorUtils.compare(input, value) > 0);
		}
	}
	
	@ConditionAnnotation
	private static class gte implements Operator
	{
		/**
		 * 比较对象
		 */
		private Object value;
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public gte(Object value) throws Exception
		{
			this.value = value;
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return (AccumulatorUtils.compare(input, value) >= 0);
		}
	}
	
	@ConditionAnnotation
	private static class lt implements Operator
	{
		/**
		 * 比较对象
		 */
		private Object value;
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public lt(Object value) throws Exception
		{
			this.value = value;
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return (AccumulatorUtils.compare(input, value) < 0);
		}
	}
	
	@ConditionAnnotation
	private static class lte implements Operator
	{
		/**
		 * 比较对象
		 */
		private Object value;
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public lte(Object value) throws Exception
		{
			this.value = value;
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return (AccumulatorUtils.compare(input, value) <= 0);
		}
	}
	
	@ConditionAnnotation
	private static class in implements Operator
	{
		/**
		 * 比较对象集（指定比较方式）
		 */
		private Set<Object> values = new TreeSet<Object>(new Comparator<Object>()
		{
			@Override
			public int compare(Object v1, Object v2)
			{
				return AccumulatorUtils.compare(v1, v2);
			}
		});
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public in(Object value) throws Exception
		{
			if (value instanceof JSONArray)
			{
				JSONArray json = (JSONArray)value;
				for (int i = 0; i < json.length(); ++i)
				{
					values.add(json.get(i));
				}
			}
			if (values.isEmpty())
			{
				throw new Exception("$in : [<value>, ....] - need to provide one or more parameters");
			}
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return values.contains(input);
		}
	}

	@ConditionAnnotation
	private static class nin implements Operator
	{
		/**
		 * 比较对象集（指定比较方式）
		 */
		private Set<Object> values = new TreeSet<Object>(new Comparator<Object>()
		{
			@Override
			public int compare(Object v1, Object v2)
			{
				return AccumulatorUtils.compare(v1, v2);
			}
		});
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public nin(Object value) throws Exception
		{
			if (value instanceof JSONArray)
			{
				JSONArray json = (JSONArray)value;
				for (int i = 0; i < json.length(); ++i)
				{
					values.add(json.get(i));
				}
			}
			if (values.isEmpty())
			{
				throw new Exception("$nin : [<value>, ....] - need to provide one or more parameters");
			}
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			return !values.contains(input);
		}
	}
	
	
	/////////////////////////////////////////////////////////////////
	//
	// 判断逻辑
	//
	/////////////////////////////////////////////////////////////////

	@ConditionAnnotation
	private static class exists implements Operator
	{
		/**
		 * 条件表达式
		 */
		private boolean condition = true;
		
		/**
		 * 构造方法
		 * @param value
		 */
		@SuppressWarnings("unused")
		public exists(Object value) throws Exception
		{
			this.condition = AccumulatorUtils.bool(value);
		}
		
		/**
		 * 执行比较操作
		 */
		@Override
		public boolean execute(Object input) throws Exception
		{
			if (this.condition)
			{
				return input != null;
			}
			else
			{
				return input == null;
			}
		}
	}

}
