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

import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;

public class CassandraBlob implements Blob {
    private final ByteBuffer _bytes;

    public CassandraBlob(byte[] bytes) {
        this(ByteBuffer.wrap(bytes == null ? new byte[0] : bytes));
    }

    public CassandraBlob(ByteBuffer bytes) {
        _bytes = bytes == null ? ByteBuffer.wrap(new byte[0]) : bytes;
    }

    public long length() throws SQLException {
        return _bytes.capacity();
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        byte[] bytes = new byte[length];
        _bytes.get(bytes, (int) pos - 1, length);
        return bytes;
    }

    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(_bytes.array());
    }

    public long position(byte[] pattern, long start) throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public long position(Blob pattern, long start) throws SQLException {
        long pos = 0L;

        try {
            pos = position(ByteStreams.toByteArray(pattern.getBinaryStream()), start);
        } catch (IOException e) {
            throw new SQLException(e);
        }

        return pos;
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        if (bytes != null) {
            _bytes.position((int) pos - 1);
            _bytes.put(bytes, offset, len);
        }

        return len;
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public void truncate(long len) throws SQLException {
        _bytes.limit((int) len);
    }

    public void free() throws SQLException {
        _bytes.clear();
    }

    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(pos, (int) length));
    }
}
