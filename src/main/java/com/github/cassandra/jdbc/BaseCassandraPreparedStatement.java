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
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This is the base class for prepared statement implementation for Cassandra.
 *
 * @author Zhichun Wu
 */
public abstract class BaseCassandraPreparedStatement extends BaseCassandraStatement {
    protected final CassandraParameterMetaData parameterMetaData;
    protected final SortedMap<Integer, Object> parameters = new TreeMap<Integer, Object>();
    protected CassandraCqlStatement cqlStmt;

    protected BaseCassandraPreparedStatement(BaseCassandraConnection conn, String cql) {
        super(conn);

        this.cqlStmt = conn == null ? null : CassandraCqlParser.parse(conn.getConfiguration(), cql);
        parameterMetaData = new CassandraParameterMetaData(conn);
    }

    protected void setParameter(int paramIndex, Object paramValue) throws SQLException {
        parameters.put(paramIndex, paramValue);
    }

    public void addBatch() throws SQLException {
        Object[] params = new Object[parameters.size()];
        int index = 0;
        for (Object p : parameters.values()) {
            params[index++] = p;
        }

        this.batch.add(new CassandraCqlStatement(cqlStmt.getCql(), cqlStmt.getConfiguration(), params));

        this.clearParameters();
    }

    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException {
        // FIXME slow and will run into OOM issue
        try {
            setBytes(parameterIndex, ByteStreams.toByteArray(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        setAsciiStream(parameterIndex, ByteStreams.limit(x, length));
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        setAsciiStream(parameterIndex, ByteStreams.limit(x, length));
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        // FIXME slow and will run into OOM issue
        try {
            setBytes(parameterIndex, ByteStreams.toByteArray(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        setBinaryStream(parameterIndex, ByteStreams.limit(x, length));
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        setBinaryStream(parameterIndex, ByteStreams.limit(x, length));
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (x == null) {
            setParameter(parameterIndex, null);
        } else {
            setBinaryStream(parameterIndex, x.getBinaryStream());
        }
    }

    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        setBinaryStream(parameterIndex, inputStream, length);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException {
        // FIXME Slow and will run into OOM issue
        try {
            setString(parameterIndex, CharStreams.toString(reader));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        // FIXME Slow and will run into OOM issue
        try {
            setString(parameterIndex, CharStreams.toString(reader).substring(0, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader,
                                   long length) throws SQLException {
        // FIXME Slow and will run into OOM issue
        try {
            setString(parameterIndex, CharStreams.toString(reader).substring(0, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setClob(parameterIndex, x == null ? null : x.getCharacterStream());
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value,
                                    long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setNCharacterStream(parameterIndex, value.getCharacterStream());
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setNCharacterStream(parameterIndex, reader);
    }

    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        setNCharacterStream(parameterIndex, reader, length);
    }

    public void setNString(int parameterIndex, String value)
            throws SQLException {
        setString(parameterIndex, value);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null, sqlType);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException {
        // FIXME
        setNull(parameterIndex, sqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        // FIXME
        setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType,
                          int scaleOrLength) throws SQLException {
        // FIXME
        setObject(parameterIndex, x);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException {
        setParameter(parameterIndex, xmlObject);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException {
        // FIXME
        setParameter(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException {
        setParameter(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException {
        // FIXME
        setParameter(parameterIndex, x);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        setAsciiStream(parameterIndex, x, length);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return parameterMetaData;
    }
}
