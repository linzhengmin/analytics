package com.appgame.analytics.aggregator.accumulator.expression;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.appgame.analytics.aggregator.accumulator.utils.AccumulatorUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class Expression
{
	
	/**
	 * 运算符标识（"$expr" : expression）
	 */
	@Retention(RetentionPolicy.RUNTIME)
	private @interface ExpressionAnnotation
	{
	}
	
	/**
	 * 定义表达式运算符
	 */
	private interface Operator
	{
		/**
		 * 运算符执行方法
		 * @param input
		 * @return
		 * @throws Exception
		 */
		public abstract Object execute(Map<String, Object> input) throws Exception;
	}
	
	/**
	 * 表达式关联运算符对象
	 */
	private Operator operator = null;
	
	/**
	 * 表达式构造方法
	 * @param operator
	 */
	private Expression(Operator operator)
	{
		this.operator = operator;
	}
	
	/**
	 * 表达式执行方法
	 * @param input
	 * @return
	 * @throws Exception 
	 */
	public Object execute(Map<String, Object> input) throws Exception
	{
		return operator.execute(input);
	}
	
	/**
	 * 定义运算符构造器
	 */
	private interface Builder
	{
		public Operator build(Object object) throws Exception;
	}
	
	/**
	 * 按配置类型选择构造器
	 */
	@SuppressWarnings("rawtypes")
	private static Map<Class, Builder> builders = new HashMap<Class, Builder>();
	static
	{
		/**
		 * 数值类型的构造参数
		 */
		builders.put(Number.class, new Builder()
		{
			@Override
			public Operator build(Object input) throws Exception
			{
				return new literals(input);
			}
		});
		
		/**
		 * 字串类型的构造参数
		 */
		builders.put(String.class, new Builder()
		{
			@Override
			public Operator build(Object input) throws Exception
			{
				String key = (String)input;
				if (key.isEmpty() || key.charAt(0) != '$')
				{
					return new literals(input);
				}
				else
				{
					return new search(key.substring(1, key.length()));
					
				}
			}
		});
		
		/**
		 * 'json'类型的构造参数
		 */
		builders.put(JSONObject.class, new Builder()
		{
			/**
			 * 构建带'Expr'标志的运算符
			 * @param key
			 * @param obj
			 * @return
			 * @throws Exception
			 */
			@SuppressWarnings({ "unchecked", "rawtypes" })
			private Operator build(String key, Object obj) throws Exception
			{
				for (Class clazz : Expression.class.getDeclaredClasses())
				{
					if (clazz.getSimpleName().equals(key) && clazz.getAnnotation(ExpressionAnnotation.class) != null)
					{
						return (Operator)(clazz.getConstructor(new Class[]{Object.class}).newInstance(obj));
					}
				}
				throw new Exception(String.format("expression - unknown operator[%s]", key));
			}
			
			@Override
			public Operator build(Object input) throws Exception
			{
				JSONObject json = (JSONObject)input;
				if (json.length() > 0)
				{
					if (json.length() == 1)
					{
						String key = (String)json.keys().next();
						if (!key.isEmpty() && key.charAt(0) == '$')
						{
							return build(key.substring(1, key.length()), json.get(key));
						}
					}
					return new mapping(json);
				}
				else
				{
					throw new Exception("expression - empty operator");
				}
			}
		});
	}
	
	/**
	 * 表达式构造方法
	 * @param input
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static Expression build(Object input) throws Exception
	{
		for (Map.Entry<Class, Builder> entry : builders.entrySet())
		{
			if (entry.getKey().isInstance(input))
			{
				return new Expression(entry.getValue().build(input));
			}
		}
		throw new Exception(String.format("expression - unknow expression[%s]", input.toString()));
	}
	
	
	
	/////////////////////////////////////////////////////////////////
	//
	// 常量运算符
	//
	/////////////////////////////////////////////////////////////////
	
	private static class literals implements Operator
	{
		/**
		 * 常量运算结果
		 */
		private Object value;
		
		/**
		 * 构造方法
		 * @param input
		 */
		public literals(Object input)
		{
			if (input.equals("nil"))
			{
				this.value = null;
			}
			else
			{
				this.value = input;
			}
		}
		
		/**
		 * 返回指定常量结果
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return value;
		}
	}
	
	
	/////////////////////////////////////////////////////////////////
	// 搜索运算符
	/////////////////////////////////////////////////////////////////
	
	private static class search implements Operator
	{
		/**
		 * 搜索路径
		 */
		private String keys[] = {};
		
		/**
		 * 构造方法
		 * @param input
		 */
		public search(String input)
		{
			this.keys = input.split("\\.");
		}
		
		/**
		 * 获取搜索结果
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Object object = input;
			for (String key : keys)
			{
				if (object instanceof Map)
				{
					object = ((Map)object).get(key);
				}
				else
				{
					return null;
				}
			}
			return object;
		}
	}
	
	
	/////////////////////////////////////////////////////////////////
	// 字段映射运算符
	/////////////////////////////////////////////////////////////////
	
	private static class mapping implements Operator
	{
		/**
		 * 映射字段集
		 */
		Map<String, Expression> keys = new HashMap<String, Expression>();

		/**
		 * 构造方法
		 * @param json
		 * @throws Exception
		 */
		@SuppressWarnings("rawtypes")
		public mapping(JSONObject json) throws Exception
		{
			for (Iterator it = json.keys(); it.hasNext(); )
			{
				String key = (String)it.next();
				if (key.isEmpty() || key.charAt(0) == '$')
				{
					throw new Exception(String.format("mapping - invalid key[%s]", key));
				}
				else
				{
					keys.put(key, Expression.build(json.get(key)));
				}
			}
		}
		
		/**
		 * 获取字段映射结果
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Map<String, Object> map = new HashMap<String, Object>();
			for (Map.Entry<String, Expression> entry : keys.entrySet())
			{
				map.put(entry.getKey(), entry.getValue().execute(input));
			}
			return map;
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 布尔逻辑
	///////////////////////////////////////////////////////////////////////////

	@ExpressionAnnotation
	private static class and implements Operator
	{
		/**
		 * 运算参数集
		 */
		private List<Expression> expressions = new ArrayList<Expression>();
		/**
		 * 构造方法
		 * @param input
		 * @throws Excepion
		 */
		@SuppressWarnings("unused")
		public and(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
				if (expressions.isEmpty())
				{
					throw new Exception("$and[<expression1>, <expression2>, ...] - need to provide one or more parameters");
				}
			}
			else
			{
				expressions.add(Expression.build(input));
			}
		}

		/**
		 * 执行操作 - 全体运算参数为'true'时返回真值
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			for (Expression expr : expressions)
			{
				if (!AccumulatorUtils.bool(expr.execute(input)))
				{
					return false;
				}
			}
			return true;
		}
	}

	@ExpressionAnnotation
	private static class or implements Operator
	{
		/**
		 * 运算参数集
		 */
		private List<Expression> expressions = new ArrayList<Expression>();

		/**
		 * 构造方法
		 * @param input
		 * @throws Excepion
		 */
		@SuppressWarnings("unused")
		public or(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
				if (expressions.isEmpty())
				{
					throw new Exception("$or[<expression1>, <expression2>, ...] - need to provide one or more parameters");
				}
			}
			else
			{
				expressions.add(Expression.build(input));
			}
		}
		
		/**
		 * 执行操作 - 任意运算参数为'true'时返回真值
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			for (Expression expr : expressions)
			{
				if (AccumulatorUtils.bool(expr.execute(input)))
				{
					return true;
				}
			}
			return false;
		}
	}

	@ExpressionAnnotation
	private static class not implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expression = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public not(Object input) throws Exception
		{
			this.expression = Expression.build(input);
		}
		
		/**
		 * 执行操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return !AccumulatorUtils.bool(expression.execute(input));
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 比较逻辑
	///////////////////////////////////////////////////////////////////////////
	
	@ExpressionAnnotation
	private static class cmp implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public cmp(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$cmp[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input));
		}
	}
	
	@ExpressionAnnotation
	private static class eq implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public eq(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$eq[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return (0 == AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input)));
		}
	}
	
	@ExpressionAnnotation
	private static class ne implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public ne(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$ne[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return (AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input)) != 0);
		}
	}
	
	@ExpressionAnnotation
	private static class gt implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public gt(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$gt[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return (AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input)) > 0);
		}
	}
	
	@ExpressionAnnotation
	private static class gte implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public gte(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$gte[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return (AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input)) >= 0);
		}
	}
	
	@ExpressionAnnotation
	private static class lt implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public lt(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$lt[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return (AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input)) < 0);
		}
	}
	
	@ExpressionAnnotation
	private static class lte implements Operator
	{
		/**
		 * 参与比较运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public lte(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$lte[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行比较操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return (AccumulatorUtils.compare(expr1.execute(input), expr2.execute(input)) <= 0);
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 算术逻辑
	///////////////////////////////////////////////////////////////////////////
	
	@ExpressionAnnotation
	private static class add implements Operator
	{
		/**
		 * 运算参数集
		 */
		private List<Expression> expressions = new ArrayList<Expression>();

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public add(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
			}
			else
			{
				expressions.add(Expression.build(input));
			}
		}
		
		/**
		 * 执行加法操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Double value = new Double(0.0);
			for (Expression expr : expressions)
			{
				Object result = expr.execute(input);
				if (result instanceof Number)
				{
					value += ((Number)result).doubleValue();
				}
			}
			return value;
		}
	}
	
	@ExpressionAnnotation
	private static class subtract implements Operator
	{
		/**
		 * 参与减法运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public subtract(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$subtract[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行减法操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Double value = new Double(0.0);
			// 初始化被减数
			Object v1 = expr1.execute(input);
			if (v1 instanceof Number)
			{
				value += ((Number) v1).doubleValue();
			}
			// 执行减法操作
			Object v2 = expr2.execute(input);
			if (v2 instanceof Number)
			{
				value -= ((Number) v2).doubleValue();
			}
			return value;
		}
	}
	
	@ExpressionAnnotation
	private static class multiply implements Operator
	{
		/**
		 * 运算参数集
		 */
		private List<Expression> expressions = new ArrayList<Expression>();

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public multiply(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
			}
			else
			{
				expressions.add(Expression.build(input));
			}
		}
		
		/**
		 * 执行乘法操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Double value = new Double(1.0);
			for (Expression expr : expressions)
			{
				Object ret = expr.execute(input);
				if (ret instanceof Number)
				{
					value = value * ((Number)ret).doubleValue();
				}
			}
			return value;
		}
	}
	
	@ExpressionAnnotation
	private static class divide implements Operator
	{
		/**
		 * 参与除法运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public divide(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$divide[<expression1>, <expression2>] - invalid parameters");
			}
		}

		/**
		 * 执行除法操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Object v1 = expr1.execute(input);
			Object v2 = expr2.execute(input);
			if (v1 instanceof Number && v2 instanceof Number)
			{
				return ((Number) v1).doubleValue() / ((Number) v2).doubleValue();
			}
			else
			{
				throw new Exception("$device - non numerical object can't perform the division operation");
			}
		}
	}
	
	@ExpressionAnnotation
	private static class mod implements Operator
	{
		/**
		 * 参与取模运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public mod(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 2)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
				}
			}
			if (expr1 == null || expr2 == null)
			{
				throw new Exception("$mod[<expression1>, <expression2>]  - invalid parameters");
			}
		}

		/**
		 * 执行取模操作
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Object v1 = expr1.execute(input);
			Object v2 = expr2.execute(input);
			if (v1 instanceof Number && v2 instanceof Number)
			{
				return ((Number) v1).doubleValue() % ((Number) v2).doubleValue();
			}
			else
			{
				throw new Exception("$mod - non numerical objects can't performed modulo operation");
			}
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 字串逻辑
	///////////////////////////////////////////////////////////////////////////
	
	@ExpressionAnnotation
	private static class concat implements Operator
	{
		/**
		 * 运算参数集
		 */
		private List<Expression> expressions = new ArrayList<Expression>();

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public concat(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
			}
			else
			{
				expressions.add(Expression.build(input));
			}
		}
		
		/**
		 * 连接字符串
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			StringBuilder builder = new StringBuilder();
			for (Expression expr : expressions)
			{
				Object ret = expr.execute(input);
				if (ret != null)
				{
					builder.append(ret.toString());
				}
				else
				{
					builder.append("null");
				}
			}
			return builder.toString();
		}
	}
	
	@ExpressionAnnotation
	private static class substring implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expr1 = null;
		private Expression expr2 = null;
		private Expression expr3 = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public substring(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 3)
				{
					expr1 = Expression.build(json.get(0));
					expr2 = Expression.build(json.get(1));
					expr3 = Expression.build(json.get(2));
				}
			}
			if (expr1 == null || expr2 == null || expr3 == null)
			{
				throw new Exception("$substring[string, start, length] : - invalid parameters");
			}
		}

		/**
		 * 获取子串
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Object v1 = expr1.execute(input);
			Object v2 = expr2.execute(input);
			Object v3 = expr3.execute(input);
			// 获取子字串
			String string = v1.toString();
			int    start  = 0;
			int    length = string.length();
			if (v2 instanceof Number)
			{
				start = ((Number) v2).intValue();
			}
			if (v3 instanceof Number)
			{
				length = ((Number) v3).intValue();
			}
			return string.substring(start, start + length);
		}
	}
	
	@ExpressionAnnotation
	private static class tolower implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expr = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public tolower(Object input) throws Exception
		{
			expr = Expression.build(input);
		}

		/**
		 * 字串转小写
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return expr.execute(input).toString().toLowerCase();
		}
	}
	
	@ExpressionAnnotation
	private static class toupper implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expr = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public toupper(Object input) throws Exception
		{ 
			expr = Expression.build(input);
		}
		
		/**
		 * 字串转大写
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return expr.execute(input).toString().toUpperCase();
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 字节数组转换
	///////////////////////////////////////////////////////////////////////////
	
	@ExpressionAnnotation
	private static class b2l implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expr = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public b2l(Object input) throws Exception
		{
			expr = Expression.build(input);
		}

		/**
		 * 字节数组转整形
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			byte bytes[] = (byte[])expr.execute(input);
			if (bytes != null && bytes.length <= 8)
			{
				long value = 0;
				for (int i = 0; i < bytes.length; ++i)
				{
					value = (value << 8) | (0x00000000000000FF & bytes[i]);
				}
				return value;
			}
			else
			{
				return 0L;
			}
		}
	}
	
	@ExpressionAnnotation
	private static class b2s implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expression = null;
		
		/**
		 *　对象缓存（引用常用字符串，降低内存消耗）
		 */
		private Cache<String, String> cache = null;
		
		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public b2s(Object input) throws Exception
		{
			// 字节转换表达式
			expression = Expression.build(input);
			// 常用字符串缓存
			cache = CacheBuilder.newBuilder()
								.maximumSize(512)
								.expireAfterAccess(30, TimeUnit.SECONDS)
								.build();
		}

		/**
		 * 字节数组转字串
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Object o = expression.execute(input);
			if (o != null)
			{
				final String v = Bytes.toString((byte[])o);
				return cache.get(v, new Callable<String>()
				{
					@Override
					public String call() throws Exception
					{
						return v;
					}
				});
			}
			else
			{
				return "null";
			}
		}
	}
	
	
	@ExpressionAnnotation
	private static class stol implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression expr = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public stol(Object input) throws Exception
		{
			expr = Expression.build(input);
		}

		/**
		 * 字符串转整形
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return  Long.parseLong(expr.execute(input).toString());
		}
	}
	
	@ExpressionAnnotation
	private static class stod implements Operator
	{
	    /**
		 * 运算参数
		 */
		private Expression expr = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public stod(Object input) throws Exception
		{
			expr = Expression.build(input);
		}

		/**
		 * 字符串转浮点
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			return  Double.parseDouble(expr.execute(input).toString());
		}
	}
	
	@ExpressionAnnotation
	private static class jscript implements Operator
	{
		/**
		 * 脚本引擎（jscript）
		 */
		private ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
		
		/**
		 * 预编译脚本
		 */
		private CompiledScript script = null;
		
		/**
		 * 构造方法
		 * @param script
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public jscript(String script) throws Exception
		{
			this.script = ((Compilable)engine).compile(script);
		}

		/**
		 * 执行脚本
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Bindings bind = engine.getBindings(ScriptContext.ENGINE_SCOPE);
			bind.clear();
			for (Map.Entry<String, Object> entry : input.entrySet())
			{
				bind.put(entry.getKey(), entry.getValue());
			}
			return script.eval();
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 条件选择
	///////////////////////////////////////////////////////////////////////////
	
	@ExpressionAnnotation
	private static class select implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression condition = null;
		private Expression v1        = null;
		private Expression v2        = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		@SuppressWarnings("unused")
		public select(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 3)
				{
					condition = Expression.build(json.get(0));
					v1        = Expression.build(json.get(1));
					v2        = Expression.build(json.get(2));
				}
			}
			if (condition == null || v1 == null || v2 == null)
			{ 
				throw new Exception("$select[<expression1>, <expression2>, <expression3>] - invalid parameters");
			}
		}

		/**
		 * 字节数组转字串
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			if (AccumulatorUtils.bool(condition.execute(input)))
			{
				return v1.execute(input);
			}
			else
			{
				return v2.execute(input);
			}
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 集合操作
	///////////////////////////////////////////////////////////////////////////

	@ExpressionAnnotation
	public static class mix implements Operator
	{
		/**
		 * 运算参数（运行多个集合间的交集运算）
		 */
		private List<Expression> expressions = new ArrayList<Expression>();
		
		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		public mix(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
			}
			if (expressions.isEmpty())
			{ 
				throw new Exception("$mix[<expression1>, <expression2>, ...] - need to provide one or more parameters");
			}
		}
		
		/**
		 * 内部交集运算（两个对象间的交集）
		 * @param source
		 * @param target
		 * @return
		 */
		@SuppressWarnings("unchecked")
		private Set<Object> inner_mix(Set<Object> source, Object target)
		{
			Set<Object> result = new HashSet<Object>();
			if (source == null)
			{
				if (target instanceof Collection)
				{
					result.addAll((Collection<Object>)target);
				}
				else
				{
					result.add(target);
				}
			}
			else
			{
				if (target instanceof Collection)
				{
					for (Object v : (Collection<Object>)target)
					{
						if (source.contains(v))
						{
							result.add(v);
						}
					}
				}
				else
				{
					if (source.contains(target))
					{
						result.add(target);
					}
				}
			}
			return result;
		}
		
		/**
		 * 交集运算
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Set<Object> result = null;
			for (Expression express : expressions)
			{
				Object v = express.execute(input);
				if (v == null)
				{
					return new HashSet<Object>();
				}
				else
				{
					result = inner_mix(result, v);
					if (result.isEmpty())
					{
						break;
					}
				}
			}
			return result;
		}
	}
	
	@ExpressionAnnotation
	public static class union implements Operator
	{
		/**
		 * 运算参数（运行多个集合间的并集运算）
		 */
		private List<Expression> expressions = new ArrayList<Expression>();
		
		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		public union(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				for (int i = 0; i < json.length(); ++i)
				{
					expressions.add(Expression.build(json.get(i)));
				}
			}
			if (expressions.isEmpty())
			{ 
				throw new Exception("$union[<expression1>, <expression2>, ...] - need to provide one or more parameters");
			}
		}

		/**
		 * 并集运算
		 */
		@SuppressWarnings("unchecked")
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Set<Object> result = new HashSet<Object>();
			for (Expression express : expressions)
			{
				Object v = express.execute(input);
				if (v != null)
				{
					if (v instanceof Collection)
					{
						result.addAll((Collection<Object>)v);
					}
					else
					{
						result.add(v);
					}
				}
			}
			return result;
		}
	}
	
	@ExpressionAnnotation
	public static class size implements Operator
	{
		/**
		 * 运算参数（计算集合内元素数量）
		 */
		private Expression expression = null;
		
		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		public size(Object input) throws Exception
		{
			this.expression = Expression.build(input);
		}
		
		/**
		 * 计算集合大小
		 */
		@SuppressWarnings("unchecked")
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			Object v = expression.execute(input);
			if (v == null)
			{
				return 0;
			}
			else
			{
				if (v instanceof Collection)
				{
					return ((Collection<Object>)v).size();
				}
				else
				{
					return 1;
				}
			}
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	// 时间/日期操作
	///////////////////////////////////////////////////////////////////////////

	@ExpressionAnnotation
	public static class formattime implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression timestamp = null;
		private Expression pattern   = null;
		private Expression zone      = null;

		/**
		 * 构造方法
		 * @param input
		 * @throws Exception
		 */
		public formattime(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() == 3 )
				{
					timestamp = Expression.build(json.get(0));
					pattern   = Expression.build(json.get(1));
					zone      = Expression.build(json.get(2));
				}
				else
				{
					timestamp = Expression.build(json.get(0));
					pattern   = Expression.build(json.get(1));
				}
			}
			if(timestamp == null ||  pattern == null)
			{
				throw new Exception("$formattime[<expression1>, <expression2>, <expression3>] - invalid parameters");
			}
		}
		
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			SimpleDateFormat formator = new SimpleDateFormat(pattern.execute(input).toString());
			if (zone != null)
			{
				formator.setTimeZone(TimeZone.getTimeZone(zone.execute(input).toString()));
			}
			return formator.format(Long.parseLong(timestamp.execute(input).toString()) * 1000);
		}
	}
	
	@ExpressionAnnotation
	public static class day implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression timestamp = null;
		private long       zone      = 0;
		
		/**
		 * 构造方法（获得指定时间戳所属日期的起始时间）
		 * @param input
		 * @throws Exception
		 */
		public day(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() >= 2)
				{
					timestamp = Expression.build(json.get(0));
					zone      = TimeZone.getTimeZone(json.get(1).toString()).getRawOffset() / 1000;
				}
				else
				{
					timestamp = Expression.build(json.get(0));
					zone      = TimeZone.getDefault().getRawOffset() / 1000;
				}
			}
			else if (input instanceof JSONObject)
			{
				timestamp = Expression.build(input);
				zone      = TimeZone.getDefault().getRawOffset() / 1000;
			}
			if(timestamp == null)
			{
				throw new Exception("$day[timestamp, zone] - invalid parameters");
			}
		}

		/**
		 * 计算时间戳所属日期的起始时间
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			long time = Long.parseLong(timestamp.execute(input).toString());
			return (time - ((time + zone) % 86400));
		}
	}
	
	@ExpressionAnnotation
	public static class between implements Operator
	{
		/**
		 * 运算参数
		 */
		private Expression timestamp1 = null;
		private Expression timestamp2 = null;
		private Expression zone       = null;
		
		/**
		 * 构造方法（获得指定时间戳所属日期的起始时间）
		 * @param input
		 * @throws Exception
		 */
		public between(Object input) throws Exception
		{
			if (input instanceof JSONArray)
			{
				JSONArray json = (JSONArray)input;
				if (json.length() >= 3)
				{
					timestamp1 = Expression.build(json.get(0));
					timestamp2 = Expression.build(json.get(1));
					zone       = Expression.build(json.get(2));
				}
				else
				{
					timestamp1 = Expression.build(json.get(0));
					timestamp2 = Expression.build(json.get(1));
				}
			}
			if(timestamp1 == null || timestamp2 == null)
			{
				throw new Exception("$between[timestamp1, timestamp2, zone] - invalid parameters");
			}
		}

		/**
		 * 计算时间戳所在日期的起始时间
		 * @param timestamp
		 * @param zone
		 * @return
		 */
		private long day(long timestamp, TimeZone zone)
		{
			Calendar calendar = Calendar.getInstance();
			if (zone != null)
			{
				calendar.setTimeZone(zone);
			}
			calendar.setTimeInMillis(timestamp * 1000);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			return calendar.getTimeInMillis() / 1000;
		}
		
		/**
		 * 计算时间戳所属日期的起始时间
		 */
		@Override
		public Object execute(Map<String, Object> input) throws Exception
		{
			if (zone != null)
			{
				return (day(Long.parseLong(timestamp1.execute(input).toString()), null) - day(Long.parseLong(timestamp2.execute(input).toString()), null)) / (24 * 3600);
			}
			else
			{
				TimeZone tzone = TimeZone.getTimeZone(zone.execute(input).toString());
				return (day(Long.parseLong(timestamp1.execute(input).toString()), tzone) - day(Long.parseLong(timestamp2.execute(input).toString()), tzone)) / (24 * 3600);
			}
		}
	}
	
}
