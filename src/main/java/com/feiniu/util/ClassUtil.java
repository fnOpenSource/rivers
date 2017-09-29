package com.feiniu.util;

import java.lang.reflect.Field;

public class ClassUtil {
	public static Class<?> getBasieClass(Class<?> c) {
		if (c.isPrimitive())
			return c;
		switch (c.getSimpleName()) {
		case "Integer":
			return int.class;
		case "Long":
			return long.class;
		case "Float":
			return float.class;
		case "Double":
			return double.class;
		case "Boolean":
			return boolean.class;
		case "Byte":
			return byte.class;
		case "Character":
			return char.class;
		default:
			return c;
		}
	}

	public static Class<?> getObjectClass(Class<?> c) {
		if (!c.isPrimitive())
			return c;
		switch (c.getSimpleName()) {
		case "short":
			return Short.class;
		case "int":
			return Integer.class;
		case "long":
			return Long.class;
		case "float":
			return Float.class;
		case "double":
			return Double.class;
		case "boolean":
			return Boolean.class;
		case "byte":
			return Byte.class;
		case "char":
			return Character.class;
		default:
			return c;
		}
	}

	/**
	 * 获得一个数据类型反射到文件中的类名,除了一些可以直接valueOf的类型外,一般都反射为String型. 然后通过重写具体类中的Set/Get处理
	 * 
	 * @param c
	 * @return
	 */
	public static Class<?> getFileReflectClass(Class<?> c) {
		if (c.isPrimitive())
			return c;
		return String.class;
	}

	/**
	 * 遍历类树, 获得相应域的类型
	 * 
	 * @param c
	 * @param fname
	 * @return
	 */
	public static Class<?> getFieldType(Class<?> c, String fname) {
		while (c != null) {
			try {
				Field field = c.getDeclaredField(fname);
				if (field != null) {
					return field.getType();
				}
			} catch (Exception e) {
			}
			c = c.getSuperclass();
		}
		return null;
	}
}
