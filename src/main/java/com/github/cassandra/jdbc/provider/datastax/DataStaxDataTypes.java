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

import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.utils.UUIDs;
import com.github.cassandra.jdbc.CassandraDataType;
import com.github.cassandra.jdbc.CassandraDataTypeConverters;
import com.github.cassandra.jdbc.CassandraDataTypeMappings;
import com.google.common.base.Function;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class DataStaxDataTypes extends CassandraDataTypeMappings {
    static final CassandraDataTypeMappings mappings = new CassandraDataTypeMappings() {
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
            addMappings(list, CassandraDataType.LIST.getTypeName(), Types.OTHER, List.class,
                    Integer.MAX_VALUE, 0);
            addMappings(list, CassandraDataType.MAP.getTypeName(), Types.OTHER, Map.class, Integer.MAX_VALUE,
                    0);
            addMappings(list, CassandraDataType.SET.getTypeName(), Types.OTHER, Set.class, Integer.MAX_VALUE,
                    0);
            addMappings(list, CassandraDataType.SMALLINT.getTypeName(), Types.SMALLINT, Short.class, 6, 0);
            addMappings(list, CassandraDataType.TEXT.getTypeName(), Types.VARCHAR, String.class, Integer.MAX_VALUE,
                    0);
            addMappings(list, CassandraDataType.TIME.getTypeName(), Types.TIME, LocalTime.class, 50, 0);
            addMappings(list, CassandraDataType.TIMESTAMP.getTypeName(), Types.TIMESTAMP, LocalDateTime.class, 50, 0);
            addMappings(list, CassandraDataType.TIMEUUID.getTypeName(), Types.VARCHAR, UUID.class, 50, 0);
            addMappings(list, CassandraDataType.TINYINT.getTypeName(), Types.TINYINT, Byte.class, 3, 0);
            addMappings(list, CassandraDataType.TUPLE.getTypeName(), Types.OTHER, TupleType.class,
                    Integer.MAX_VALUE, 0);
            addMappings(list, CassandraDataType.UUID.getTypeName(), Types.VARCHAR, UUID.class, 50, 0);
            addMappings(list, CassandraDataType.VARCHAR.getTypeName(), Types.VARCHAR, String.class,
                    Integer.MAX_VALUE, 0);
            addMappings(list, CassandraDataType.VARINT.getTypeName(), Types.BIGINT, BigInteger.class,
                    Integer.MAX_VALUE, 0);
        }
    };

    static final CassandraDataTypeConverters converters = new CassandraDataTypeConverters() {
        @Override
        protected void init() {
            super.init();

            // add / override converters
            addMapping(com.datastax.driver.core.LocalDate.class,
                    com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()),
                    new Function<Object, com.datastax.driver.core.LocalDate>() {
                        public com.datastax.driver.core.LocalDate apply(Object input) {
                            com.datastax.driver.core.LocalDate date;
                            if (input instanceof java.util.Date) {
                                date = com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(
                                        ((java.util.Date) input).getTime());
                            } else {
                                date = com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(
                                        Date.valueOf(String.valueOf(input)).getTime());
                            }
                            return date;
                        }
                    });

            // Use DataStax UUIDs to generate time-based UUID
            addMapping(java.util.UUID.class, UUIDs.timeBased(), new Function<Object, UUID>() {
                public UUID apply(Object input) {
                    return java.util.UUID.fromString(String.valueOf(input));
                }
            });

            // workaround for Date, Time and Timestamp
            addMapping(Date.class, new Date(System.currentTimeMillis()),
                    new Function<Object, Date>() {
                        public Date apply(Object input) {
                            Date date;
                            if (input instanceof com.datastax.driver.core.LocalDate) {
                                com.datastax.driver.core.LocalDate localDate =
                                        (com.datastax.driver.core.LocalDate) input;
                                date = new Date(new LocalDate(
                                        localDate.getYear(), localDate.getMonth(),
                                        localDate.getDay()).toDate().getTime());
                            } else if (input instanceof LocalDate) {
                                date = new Date(((LocalDate) input).toDate().getTime());
                            } else if (input instanceof java.util.Date) {
                                date = new Date(((java.util.Date) input).getTime());
                            } else if (input instanceof Number) {
                                date = new Date(((Number) input).longValue());
                            } else {
                                date = Date.valueOf(String.valueOf(input));
                            }
                            return date;
                        }
                    });
            addMapping(LocalTime.class, LocalTime.now(), new Function<Object, LocalTime>() {
                public LocalTime apply(Object input) {
                    LocalTime result;

                    if (input instanceof java.util.Date) {
                        result = new LocalTime(((java.util.Date) input).getTime());
                    } else if (input instanceof Number) {
                        // this is a bit tricky as the number usually represents nanoseconds since midnight
                        // http://docs.datastax.com/en/latest-java-driver/java-driver/reference/javaClass2Cql3Datatypes.html?scroll=cql-java-types__date-section
                        long possibllyNanoSeconds = ((Number) input).longValue();
                        // FIXME probably don't need to go this far...
                        if (possibllyNanoSeconds % 1000000 == 0) {
                            possibllyNanoSeconds = possibllyNanoSeconds / 1000000;
                        }
                        result = new LocalTime(LocalDate.now().toDateTimeAtStartOfDay().getMillis()
                                + possibllyNanoSeconds);
                    } else {
                        result = LocalTime.parse(String.valueOf(input));
                    }

                    return result;
                }
            });
            addMapping(Time.class, new Time(System.currentTimeMillis()),
                    new Function<Object, Time>() {
                        public Time apply(Object input) {
                            Time time;
                            if (input instanceof LocalTime) {
                                time = new Time(((LocalTime) input).toDateTimeToday().getMillis());
                            } else if (input instanceof java.util.Date) {
                                time = new Time(((java.util.Date) input).getTime());
                            } else if (input instanceof Number) {
                                // this is a bit tricky as the number usually represents nanoseconds since midnight
                                // http://docs.datastax.com/en/latest-java-driver/java-driver/reference/javaClass2Cql3Datatypes.html?scroll=cql-java-types__date-section
                                long possibllyNanoSeconds = ((Number) input).longValue();
                                // FIXME probably don't need to go this far...
                                if (possibllyNanoSeconds % 1000000 == 0) {
                                    possibllyNanoSeconds = possibllyNanoSeconds / 1000000;
                                }
                                time = new Time(
                                        LocalTime.fromMillisOfDay(possibllyNanoSeconds).toDateTimeToday().getMillis());
                            } else {
                                time = new Time(LocalTime.parse(String.valueOf(input)).toDateTimeToday().getMillis());
                            }
                            return time;
                        }
                    });
        }
    };
}
