/**
 * Copyright (C) 2015-2016, Zhichun Wu
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
package com.github.cassandra.jdbc.cql;

import com.github.cassandra.jdbc.CassandraCqlStmtConfiguration;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.statements.SelectStatement;

public class CqlSelectFormatter {
    public String format(CassandraCqlStmtConfiguration stmtConfig, SelectStatement.RawStatement select) {
        // select.getLimit()
        StringBuilder builder = new StringBuilder("SELECT ");
        if (select.selectClause.size() == 0) {
            builder.append("* ");
        } else {
            for (RawSelector selector : select.selectClause) {
                builder.append(selector.alias.toCQLString()).append(", ");
            }
        }
        builder.append("FROM ").append(select.columnFamily());
        if (select.whereClause.expressions.size() > 0 || select.whereClause.relations.size() > 0) {
            builder.append("WHERE ");



            if (select.parameters.allowFiltering) {
                builder.append("ALLOW FILTERING");
            }
        }

        return builder.toString();
    }
}
