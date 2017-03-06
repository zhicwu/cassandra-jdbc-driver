/**
 * Copyright (C) 2015-2017, Zhichun Wu
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
 */
package com.github.cassandra.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;

/**
 * This defines mappings to get SQL/Java type based on given CQL type.
 *
 * @author Zhichun Wu
 */
public class CassandraDataTypeMappings {
    public static final CassandraDataTypeMappings instance = new CassandraDataTypeMappings();

    private final Map<String, Class<?>> cql2JavaMapping = new HashMap<String, Class<?>>();
    private final Map<String, Integer> cql2JdbcMapping = new HashMap<String, Integer>();
    private final Map<String, Integer> precisionMapping = new HashMap<String, Integer>();
    private final Map<String, Integer> scaleMapping = new HashMap<String, Integer>();

    private final Object[][] typeMetaData;

    protected void init(List<Object[]> list) {
        addMappings(list, CassandraDataType.ASCII.getTypeName(), Types.VARCHAR, String.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.BIGINT.getTypeName(), Types.BIGINT, Long.class, 20, 0);
        addMappings(list, CassandraDataType.BLOB.getTypeName(), Types.BLOB, byte[].class, Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.BOOLEAN.getTypeName(), Types.BOOLEAN, Boolean.class, 1, 0);
        addMappings(list, CassandraDataType.COUNTER.getTypeName(), Types.BIGINT, Long.class, 20, 0);
        addMappings(list, CassandraDataType.DATE.getTypeName(), Types.DATE, java.sql.Date.class, 10, 0);
        addMappings(list, CassandraDataType.DECIMAL.getTypeName(), Types.DECIMAL, BigDecimal.class,
                10, 0);
        addMappings(list, CassandraDataType.DOUBLE.getTypeName(), Types.DOUBLE, Double.class, 22, 31);
        addMappings(list, CassandraDataType.FLOAT.getTypeName(), Types.FLOAT, Float.class, 12, 31);
        addMappings(list, CassandraDataType.INET.getTypeName(), Types.VARCHAR, InetAddress.class, 200, 0);
        addMappings(list, CassandraDataType.INT.getTypeName(), Types.INTEGER, Integer.class, 10, 0);
        addMappings(list, CassandraDataType.LIST.getTypeName(), Types.VARCHAR, List.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.MAP.getTypeName(), Types.VARCHAR, Map.class, Integer.MAX_VALUE,
                0);
        addMappings(list, CassandraDataType.SET.getTypeName(), Types.VARCHAR, Set.class, Integer.MAX_VALUE,
                0);
        addMappings(list, CassandraDataType.SMALLINT.getTypeName(), Types.SMALLINT, Short.class, 6, 0);
        addMappings(list, CassandraDataType.TEXT.getTypeName(), Types.VARCHAR, String.class, Integer.MAX_VALUE,
                0);
        addMappings(list, CassandraDataType.TIME.getTypeName(), Types.TIME, Time.class, 8, 0);
        addMappings(list, CassandraDataType.TIMESTAMP.getTypeName(), Types.TIMESTAMP, Timestamp.class, 19, 0);
        addMappings(list, CassandraDataType.TIMEUUID.getTypeName(), Types.CHAR, UUID.class, 36, 0);
        addMappings(list, CassandraDataType.TINYINT.getTypeName(), Types.TINYINT, Byte.class, 4, 0);
        addMappings(list, CassandraDataType.TUPLE.getTypeName(), Types.OTHER, Object.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.UUID.getTypeName(), Types.CHAR, UUID.class, 36, 0); // UUID1
        addMappings(list, CassandraDataType.VARCHAR.getTypeName(), Types.VARCHAR, String.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.VARINT.getTypeName(), Types.BIGINT, BigInteger.class,
                Integer.MAX_VALUE, 0);
    }

    protected CassandraDataTypeMappings() {
        List<Object[]> list = new ArrayList<Object[]>();

        init(list);

        typeMetaData = new Object[list.size()][];
        int index = 0;
        for (Object[] objs : list) {
            typeMetaData[index++] = objs;
        }
    }

    protected void addMappings(List<Object[]> list, String cqlType, int sqlType,
                               Class<?> javaType, int precision, int scale) {
        cql2JdbcMapping.put(cqlType, sqlType);
        cql2JavaMapping.put(cqlType, javaType);
        precisionMapping.put(cqlType, precision);
        scaleMapping.put(cqlType, scale);

        list.add(new Object[]{cqlType, // TYPE_NAME
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

    protected Object[][] getTypeMetaData() {
        return typeMetaData;
    }

    public String cqlTypeFor(String cqlType) {
        String recognizedCqlType;

        if (cql2JdbcMapping.containsKey(cqlType)) {
            recognizedCqlType = cqlType;
        } else if (cqlType.startsWith(CassandraDataType.LIST.getTypeName())) {
            recognizedCqlType = CassandraDataType.LIST.getTypeName();
        } else if (cqlType.startsWith(CassandraDataType.SET.getTypeName())) {
            recognizedCqlType = CassandraDataType.SET.getTypeName();
        } else if (cqlType.startsWith(CassandraDataType.MAP.getTypeName())) {
            recognizedCqlType = CassandraDataType.MAP.getTypeName();
        } else if (cqlType.startsWith(CassandraDataType.TUPLE.getTypeName())) {
            recognizedCqlType = CassandraDataType.TUPLE.getTypeName();
        } else {
            recognizedCqlType = CassandraDataType.BLOB.getTypeName();
        }

        return recognizedCqlType;
    }

    public Class<?> javaTypeFor(String cqlType) {
        return cql2JavaMapping.containsKey(cqlType) ? cql2JavaMapping
                .get(cqlType) : String.class;
    }

    public int precisionFor(String cqlType) {
        return precisionMapping.containsKey(cqlType) ? precisionMapping
                .get(cqlType) : 0;
    }

    public int scaleFor(String cqlType) {
        return scaleMapping.containsKey(cqlType) ? scaleMapping.get(cqlType)
                : 0;
    }

    public int sqlTypeFor(String cqlType) {
        return cql2JdbcMapping.containsKey(cqlType) ? cql2JdbcMapping
                .get(cqlType) : Types.VARCHAR;
    }
}
