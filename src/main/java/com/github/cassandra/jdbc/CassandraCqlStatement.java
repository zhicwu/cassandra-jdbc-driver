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

import com.google.common.base.Objects;
import com.google.common.base.Strings;

public class CassandraCqlStatement {
    private final String cql;
    // based on first line comments - the magic comment, for example:
    // -- fetchSize=100, readTimeout=10
    // select * from system.peers
    // private final String inlineConfiguration = null;

    // additional fields parsed from given SQL(not CQL), for example:
    // select *, 1 as from system.peers
    // private final SortedMap<String, Integer> name2type;
    private final CassandraCqlStmtConfiguration config;
    private final Object[] parameters;

    public CassandraCqlStatement(String cql, CassandraCqlStmtConfiguration config, Object... params) {
        this.cql = Strings.nullToEmpty(cql);
        this.config = config;

        this.parameters = new Object[params == null ? 0 : params.length];

        if (params != null) {
            int index = 0;
            for (Object p : params) {
                this.parameters[index++] = p;
            }
        }
    }

    public String getCql() {
        return this.cql;
    }

    public CassandraCqlStmtConfiguration getConfiguration() {
        return this.config;
    }

    public boolean hasParameter() {
        return this.parameters.length > 0;
    }

    public Object[] getParameters() {
        Object[] params = new Object[parameters.length];
        System.arraycopy(parameters, 0, params, 0, parameters.length);
        return params;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.cql, this.config, this.parameters);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CassandraCqlStatement other = (CassandraCqlStatement) obj;

        return Objects.equal(this.cql, other.cql)
                && Objects.equal(this.config, other.config)
                && Objects.equal(this.parameters, other.parameters);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(this.cql)
                .addValue(this.config)
                .addValue(this.parameters)
                .toString();
    }
}
