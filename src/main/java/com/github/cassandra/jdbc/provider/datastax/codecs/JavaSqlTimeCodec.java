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
package com.github.cassandra.jdbc.provider.datastax.codecs;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.LocalTime;

import java.nio.ByteBuffer;
import java.sql.Time;

import static com.datastax.driver.core.ParseUtils.quote;

public class JavaSqlTimeCodec extends TypeCodec<Time> {
    public static final JavaSqlTimeCodec instance = new JavaSqlTimeCodec();

    private JavaSqlTimeCodec() {
        super(DataType.time(), Time.class);
    }

    @Override
    public ByteBuffer serialize(Time value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        return bigint().serializeNoBoxing(new LocalTime(value.getTime()).getMillisOfDay() * 1000000L, protocolVersion);
    }

    @Override
    public Time deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        long nanosOfDay = bigint().deserializeNoBoxing(bytes, protocolVersion);
        return new Time(LocalTime.fromMillisOfDay(nanosOfDay / 1000000L).toDateTimeToday().getMillis());
    }

    @Override
    public String format(Time value) {
        if (value == null)
            return "NULL";
        return quote(new LocalTime(value.getTime()).toString());
    }

    @Override
    public Time parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // enclosing single quotes required, even for long literals
        if (!ParseUtils.isQuoted(value))
            throw new InvalidTypeException("time values must be enclosed by single quotes");
        value = value.substring(1, value.length() - 1);

        if (ParseUtils.isLongLiteral(value)) {
            long nanosOfDay;
            try {
                nanosOfDay = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value), e);
            }
            return new Time(LocalTime.fromMillisOfDay(nanosOfDay / 1000000L).getMillisOfSecond());
        }

        try {
            return new Time(LocalTime.parse(value).toDateTimeToday().getMillis());
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value), e);
        }
    }

}
