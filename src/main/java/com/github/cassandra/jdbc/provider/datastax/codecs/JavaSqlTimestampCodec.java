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
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import static com.datastax.driver.core.ParseUtils.*;
import static java.lang.Long.parseLong;

public class JavaSqlTimestampCodec extends TypeCodec<Timestamp> {
    public static final JavaSqlTimestampCodec instance = new JavaSqlTimestampCodec();

    /**
     * A {@link DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final DateTimeFormatter PARSER = new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.dateOptionalTimeParser().getParser())
            .appendOptional(
                    new DateTimeFormatterBuilder()
                            .appendTimeZoneOffset("Z", true, 2, 4)
                            .toParser())
            .toFormatter()
            .withZoneUTC();

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withZoneUTC();

    private JavaSqlTimestampCodec() {
        super(DataType.timestamp(), Timestamp.class);
    }

    @Override
    public ByteBuffer serialize(Timestamp value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigint().serializeNoBoxing(value.getTime(), protocolVersion);
    }

    @Override
    public Timestamp deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        long millis = bigint().deserializeNoBoxing(bytes, protocolVersion);
        return new Timestamp(millis);
    }

    @Override
    public String format(Timestamp value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.print(new Instant(value.getTime())));
    }

    @Override
    public Timestamp parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        // strip enclosing single quotes, if any
        if (isQuoted(value))
            value = unquote(value);
        if (isLongLiteral(value)) {
            try {
                return new Timestamp(parseLong(value));
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        try {
            return new Timestamp(PARSER.parseMillis(value));
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }
}
