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
package com.github.cassandra.jdbc.provider.datastax;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TupleType;
import com.github.cassandra.jdbc.CassandraDataType;
import com.github.cassandra.jdbc.CassandraDataTypeMappings;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class DataStaxDataTypeMappings extends CassandraDataTypeMappings {
    static final CassandraDataTypeMappings instance = new DataStaxDataTypeMappings();

    @Override
    protected void init(List<Object[]> list) {
        // http://docs.datastax.com/en/latest-java-driver/java-driver/reference/javaClass2Cql3Datatypes.html
        addMappings(list, CassandraDataType.ASCII.getTypeName(), Types.VARCHAR, String.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.BIGINT.getTypeName(), Types.BIGINT, Long.class, 19, 0);
        addMappings(list, CassandraDataType.BLOB.getTypeName(), Types.BLOB, ByteBuffer.class, Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.BOOLEAN.getTypeName(), Types.BOOLEAN, Boolean.class, 4, 0);
        addMappings(list, CassandraDataType.COUNTER.getTypeName(), Types.BIGINT, Long.class, 19, 0);
        addMappings(list, CassandraDataType.DATE.getTypeName(), Types.DATE, LocalDate.class, 10, 0);
        addMappings(list, CassandraDataType.DECIMAL.getTypeName(), Types.DECIMAL, BigDecimal.class,
                Integer.MAX_VALUE, 2);
        addMappings(list, CassandraDataType.DOUBLE.getTypeName(), Types.DOUBLE, Double.class, 22, 8);
        addMappings(list, CassandraDataType.FLOAT.getTypeName(), Types.FLOAT, Float.class, 12, 4);
        addMappings(list, CassandraDataType.INET.getTypeName(), Types.VARCHAR, InetAddress.class, 200, 0);
        addMappings(list, CassandraDataType.INT.getTypeName(), Types.INTEGER, Integer.class, 10, 0);
        addMappings(list, CassandraDataType.LIST.getTypeName(), Types.JAVA_OBJECT, List.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.MAP.getTypeName(), Types.JAVA_OBJECT, Map.class, Integer.MAX_VALUE,
                0);
        addMappings(list, CassandraDataType.SET.getTypeName(), Types.JAVA_OBJECT, Set.class, Integer.MAX_VALUE,
                0);
        addMappings(list, CassandraDataType.SMALLINT.getTypeName(), Types.SMALLINT, Short.class, 6, 0);
        addMappings(list, CassandraDataType.TEXT.getTypeName(), Types.VARCHAR, String.class, Integer.MAX_VALUE,
                0);
        addMappings(list, CassandraDataType.TIME.getTypeName(), Types.TIME, Time.class, 50, 0);
        addMappings(list, CassandraDataType.TIMESTAMP.getTypeName(), Types.TIMESTAMP, Timestamp.class, 50, 0);
        addMappings(list, CassandraDataType.TIMEUUID.getTypeName(), Types.VARCHAR, UUID.class, 50, 0);
        addMappings(list, CassandraDataType.TINYINT.getTypeName(), Types.TINYINT, Byte.class, 3, 0);
        addMappings(list, CassandraDataType.TUPLE.getTypeName(), Types.JAVA_OBJECT, TupleType.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.UUID.getTypeName(), Types.VARCHAR, UUID.class, 50, 0);
        addMappings(list, CassandraDataType.VARCHAR.getTypeName(), Types.VARCHAR, String.class,
                Integer.MAX_VALUE, 0);
        addMappings(list, CassandraDataType.VARINT.getTypeName(), Types.BIGINT, BigInteger.class, Integer.MAX_VALUE, 0);
    }
}
