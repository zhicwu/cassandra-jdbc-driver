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
import com.datastax.driver.core.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BytesBlobCodec extends TypeCodec<byte[]> {
    public static final BytesBlobCodec instance = new BytesBlobCodec();

    private BytesBlobCodec() {
        super(DataType.blob(), byte[].class);
    }

    @Override
    public ByteBuffer serialize(byte[] value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return value == null ? null : ByteBuffer.wrap(Arrays.copyOf(value, value.length));
    }

    @Override
    public byte[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null || bytes.remaining() == 0)
            return null;

        return bytes.duplicate().array();
    }

    @Override
    public String format(byte[] value) {
        if (value == null)
            return "NULL";
        return Bytes.toHexString(value);
    }

    @Override
    public byte[] parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                ? null : Bytes.fromHexString(value).array();
    }

}
