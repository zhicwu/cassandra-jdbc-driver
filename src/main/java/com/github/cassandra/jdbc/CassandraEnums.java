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

public final class CassandraEnums {
    public static enum Batch {
        UNKNOWN, // let the provider to decided
        LOGGED,
        UNLOGGED;
    }

    public static enum ConsistencyLevel {
        UNKNOWN, // just use the current consistency level
        ANY,
        ONE,
        TWO,
        THREE,
        QUORUM,
        ALL,
        LOCAL_QUORUM,
        EACH_QUORUM,
        SERIAL,
        LOCAL_SERIAL,
        LOCAL_ONE;
    }

    public static enum Compression {
        NONE,
        LZ4,
        SNAPPY;
    }

    public static enum DataType {
        ASCII("ascii"),
        BIGINT("bigint"),
        BLOB("blob"),
        BOOLEAN("boolean"),
        COUNTER("counter"),
        DATE("date"),
        DECIMAL("decimal"),
        DOUBLE("double"),
        FLOAT("float"),
        INET("inet"),
        INT("int"),
        LIST("list"),
        MAP("map"),
        SET("set"),
        SMALLINT("smallint"),
        TEXT("text"),
        TIME("time"),
        TIMESTAMP("timestamp"),
        TIMEUUID("timeuuid"),
        TINYINT("tinyint"),
        TUPLE("tuple"),
        UUID("uuid"),
        VARCHAR("varchar"),
        VARINT("varint");

        private final String typeName;

        private DataType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }
    }

    private CassandraEnums() {
    }
}
