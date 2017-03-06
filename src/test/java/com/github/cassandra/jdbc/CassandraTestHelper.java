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

import com.datastax.driver.core.LocalDate;
import org.joda.time.Instant;
import org.joda.time.LocalTime;

import java.sql.Time;
import java.util.Date;

public class CassandraTestHelper {
    private static CassandraTestHelper instance = new CassandraTestHelper();

    private static class Cassandra21TestHelper extends CassandraTestHelper {
        public String replaceScript(String script) {
            script = script.replaceAll("([\\s<])smallint([\\s>,])", "$1int$2");
            script = script.replaceAll("([\\s<])tinyint([\\s>,])", "$1int$2");
            script = script.replaceAll("([\\s<])date([\\s>,])", "$1timestamp$2");
            script = script.replaceAll("([\\s<])time([\\s>,])", "$1timestamp$2");

            return script;
        }

        public String replaceStatement(String statement) {
            statement = statement.replace("'13:30:54.234'", "'1970-01-01 13:30:54.234'");

            return statement;
        }

        public Class replaceDataType(Class clazz, Object value) {
            if (LocalDate.class.equals(clazz)) {
                clazz = Date.class;
            } else if (Short.class.equals(clazz) || Byte.class.equals(clazz)) {
                clazz = Integer.class;
            } else if (value instanceof Date && Long.class.equals(clazz)) {
                clazz = Date.class;
            }

            return clazz;
        }

        public Object replaceParameter(Object parameter, Class clazz) {
            if (parameter instanceof Short && Short.class.equals(clazz)) {
                parameter = ((Short) parameter).intValue();
            } else if (parameter instanceof Byte && Byte.class.equals(clazz)) {
                parameter = ((Byte) parameter).intValue();
            } else if (parameter instanceof LocalDate && java.sql.Date.class.equals(clazz)) {
                parameter = new Date(java.sql.Date.valueOf(String.valueOf(parameter)).getTime());
            } else if (parameter instanceof String && Time.class.equals(clazz)) {
                parameter = LocalTime.parse((String) parameter).toDateTimeToday().toString();
            } else if (parameter instanceof Long && Time.class.equals(clazz)) {
                parameter = LocalTime.fromMillisOfDay((Long) parameter / 1000000L).toDateTimeToday().getMillis();
            } else if (parameter instanceof Time && Time.class.equals(clazz)) {
                parameter = LocalTime.fromDateFields((Time) parameter).toDateTimeToday().toDate();
            } else if (parameter instanceof LocalTime && Time.class.equals(clazz)) {
                parameter = ((LocalTime) parameter).toDateTimeToday().toString();
            }

            return parameter;
        }

        public Object replaceResult(Object value, Class clazz) {
            if (value instanceof Date && LocalDate.class.equals(clazz)) {
                org.joda.time.LocalDate d = org.joda.time.LocalDate.fromDateFields((Date) value);
                value = LocalDate.fromYearMonthDay(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth());
            } else if (value instanceof String && LocalDate.class.equals(clazz)) {
                // this is kind of tricky...
                org.joda.time.LocalDate d = org.joda.time.LocalDate.fromDateFields(
                        Instant.parse((String) value).toDate());
                value = d.toString();
            } else if (value instanceof String && Time.class.equals(clazz)) {
                value = Instant.parse((String) value).toDateTime().toLocalTime().toString();
            } else if (value instanceof Date && Time.class.equals(clazz)) {
                value = LocalTime.fromDateFields((Date) value).getMillisOfDay() * 1000000L;
            }

            return value;
        }
    }

    public static void init(int majorVersion, int minorVersion) {
        // Cassandra 2.1 does not support date, time, smallint and tinyint
        if (majorVersion == 2 && minorVersion <= 1) {
            instance = new Cassandra21TestHelper();
        }
    }

    public static CassandraTestHelper getInstance() {
        return instance;
    }

    public String replaceScript(String script) {
        return script;
    }

    public String replaceStatement(String statement) {
        return statement;
    }

    public Class replaceDataType(Class clazz, Object value) {
        return clazz;
    }

    public Object replaceParameter(Object parameter, Class clazz) {
        return parameter;
    }

    public Object replaceResult(Object result, Class clazz) {
        return result;
    }
}
