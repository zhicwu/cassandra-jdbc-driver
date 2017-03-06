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
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;
import java.sql.Date;

import static com.datastax.driver.core.CodecUtils.*;
import static com.datastax.driver.core.ParseUtils.*;
import static javax.xml.bind.DatatypeConverter.parseLong;
import static org.joda.time.Days.daysBetween;

public class JavaSqlDateCodec extends TypeCodec<Date> {
    public static final JavaSqlDateCodec instance = new JavaSqlDateCodec();

    private static final LocalDate EPOCH = new LocalDate(1970, 1, 1);

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();

    private JavaSqlDateCodec() {
        super(DataType.date(), Date.class);
    }

    @Override
    public ByteBuffer serialize(Date value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        Days days = daysBetween(EPOCH, LocalDate.fromDateFields(value));
        int unsigned = fromSignedToUnsignedInt(days.getDays());
        return cint().serializeNoBoxing(unsigned, protocolVersion);
    }

    @Override
    public Date deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
        int signed = fromUnsignedToSignedInt(unsigned);
        return new Date(EPOCH.plusDays(signed).toDate().getTime());
    }

    @Override
    public String format(Date value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.print(LocalDate.fromDateFields(value)));
    }

    @Override
    public Date parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (isQuoted(value)) {
            value = unquote(value);
        }

        if (isLongLiteral(value)) {
            long raw;
            try {
                raw = parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            int days;
            try {
                days = fromCqlDateToDaysSinceEpoch(raw);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            return new Date(EPOCH.plusDays(days).toDate().getTime());
        }

        try {
            return new Date(LocalDate.parse(value, FORMATTER).toDate().getTime());
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
        }
    }

}
