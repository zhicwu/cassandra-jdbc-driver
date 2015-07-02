/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package com.github.cassandra.jdbc;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This defines mappings to get SQL/Java type based on given CQL type.
 *
 * @author Zhichun Wu
 */
public class CassandraDataTypeMappings {
	public static final String ASCII = "ascii";
	public static final String BIGINT = "bigint";
	public static final String BLOB = "blob";
	public static final String BOOLEAN = "boolean";
	public static final String COUNTER = "counter";
	public static final String DECIMAL = "decimal";
	public static final String DOUBLE = "double";
	public static final String FLOAT = "float";
	public static final String INET = "inet";
	public static final String INT = "int";
	public static final String LIST = "list";
	public static final String MAP = "map";
	public static final String SET = "set";
	public static final String TEXT = "text";
	public static final String TIMESTAMP = "timestamp";
	public static final String TIMEUUID = "timeuuid";
	public static final String TUPLE = "tuple";
	public static final String UUID = "uuid";
	public static final String VARCHAR = "varchar";
	public static final String VARINT = "varint";

	public static final Object[][] TYPE_META_DATA;

	static final Map<String, Integer> CQL2SQL_MAPPING = new HashMap<String, Integer>();
	static final Map<String, Class<?>> CQL2JAVA_MAPPING = new HashMap<String, Class<?>>();

	static final Map<String, Integer> PRECISION_MAPPING = new HashMap<String, Integer>();
	static final Map<String, Integer> SCALE_MAPPING = new HashMap<String, Integer>();

	static {
		List<Object[]> list = new ArrayList<Object[]>();

		addMappings(list, ASCII, Types.VARCHAR, String.class,
				Integer.MAX_VALUE, 0);
		addMappings(list, BIGINT, Types.BIGINT, Long.class, 19, 0);
		addMappings(list, BLOB, Types.BLOB, String.class, Integer.MAX_VALUE, 0);
		addMappings(list, BOOLEAN, Types.BOOLEAN, Boolean.class, 4, 0);
		addMappings(list, COUNTER, Types.BIGINT, Long.class, 19, 0);
		addMappings(list, DECIMAL, Types.DECIMAL, BigDecimal.class,
				Integer.MAX_VALUE, 2);
		addMappings(list, DOUBLE, Types.DOUBLE, Double.class, 22, 8);
		addMappings(list, FLOAT, Types.FLOAT, Float.class, 12, 4);
		addMappings(list, INET, Types.VARCHAR, InetAddress.class, 200, 0);
		addMappings(list, INT, Types.INTEGER, Integer.class, 10, 0);
		addMappings(list, LIST, Types.JAVA_OBJECT, List.class,
				Integer.MAX_VALUE, 0);
		addMappings(list, MAP, Types.JAVA_OBJECT, Map.class, Integer.MAX_VALUE,
				0);
		addMappings(list, SET, Types.JAVA_OBJECT, Set.class, Integer.MAX_VALUE,
				0);
		addMappings(list, TEXT, Types.VARCHAR, String.class, Integer.MAX_VALUE,
				0);
		addMappings(list, TIMESTAMP, Types.TIMESTAMP, Timestamp.class, 50, 0);
		addMappings(list, TIMEUUID, Types.VARCHAR, String.class, 50, 0);
		addMappings(list, TUPLE, Types.JAVA_OBJECT, Object.class,
				Integer.MAX_VALUE, 0);
		addMappings(list, UUID, Types.VARCHAR, String.class, 50, 0);
		addMappings(list, VARCHAR, Types.VARCHAR, String.class,
				Integer.MAX_VALUE, 0);
		addMappings(list, VARINT, Types.INTEGER, Integer.class, 10, 0);

		TYPE_META_DATA = new Object[list.size()][];
		int index = 0;
		for (Object[] objs : list) {
			TYPE_META_DATA[index++] = objs;
		}
	}

	static void addMappings(List<Object[]> list, String cqlType, int sqlType,
			Class<?> javaType, int precision, int scale) {
		CQL2SQL_MAPPING.put(cqlType, sqlType);
		CQL2JAVA_MAPPING.put(cqlType, javaType);
		PRECISION_MAPPING.put(cqlType, precision);
		SCALE_MAPPING.put(cqlType, scale);

		list.add(new Object[] { cqlType, // TYPE_NAME
				sqlType, // DATA_TYPE
				0, // PRECISION
				null, // LITERAL_PREFIX
				null, // LITERAL_SUFFIX
				null, // CREATE_PARAMS
				java.sql.DatabaseMetaData.typeNullable, // NULLABLE
				true, // CASE_SENSITIVE
				java.sql.DatabaseMetaData.typePredNone, // SEARCHABLE
				false, // UNSIGNED_ATTRIBUTE
				false, // FIXED_PREC_SCALE
				false, // AUTO_INCREMENT
				null, // LOCAL_TYPE_NAME
				0, // MINIMUM_SCALE
				0, // MAXIMUM_SCALE
				0, // SQL_DATA_TYPE
				0, // SQL_DATETIME_SUB
				10 // NUM_PREC_RADIX
		});
	}

	public static String cqlTypeFor(String cqlType) {
		return CQL2SQL_MAPPING.containsKey(cqlType) ? cqlType : TEXT;
	}

	public static int sqlTypeFor(String cqlType) {
		return CQL2SQL_MAPPING.containsKey(cqlType) ? CQL2SQL_MAPPING
				.get(cqlType) : Types.VARCHAR;
	}

	public static Class<?> javaTypeFor(String cqlType) {
		return CQL2JAVA_MAPPING.containsKey(cqlType) ? CQL2JAVA_MAPPING
				.get(cqlType) : String.class;
	}

	public static int precisionFor(String cqlType) {
		return PRECISION_MAPPING.containsKey(cqlType) ? PRECISION_MAPPING
				.get(cqlType) : 0;
	}

	public static int scaleFor(String cqlType) {
		return SCALE_MAPPING.containsKey(cqlType) ? SCALE_MAPPING.get(cqlType)
				: 0;
	}
}
