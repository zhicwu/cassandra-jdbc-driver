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
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.CodecUtils.fromSignedToUnsignedInt;
import static com.datastax.driver.core.CodecUtils.fromUnsignedToSignedInt;
import static com.datastax.driver.core.ParseUtils.*;
import static org.joda.time.Days.daysBetween;

public class StringDateCodec extends TypeCodec<String> {
    public static final StringDateCodec instance = new StringDateCodec();

    private static final LocalDate EPOCH = new LocalDate(1970, 1, 1);

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();

    private StringDateCodec() {
        super(DataType.date(), String.class);
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        Days days = daysBetween(EPOCH, LocalDate.parse(value));
        int unsigned = fromSignedToUnsignedInt(days.getDays());
        return cint().serializeNoBoxing(unsigned, protocolVersion);
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
        int signed = fromUnsignedToSignedInt(unsigned);
        return FORMATTER.print(EPOCH.plusDays(signed));
    }

    @Override
    public String format(String value) {
        return value == null ? "NULL" : quote(value);
    }

    @Override
    public String parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (isQuoted(value)) {
            value = unquote(value);
        }

        return value;
    }

}
