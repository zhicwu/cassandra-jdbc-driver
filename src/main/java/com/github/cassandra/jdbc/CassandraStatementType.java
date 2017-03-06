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

public enum CassandraStatementType {
    CREATE("DDL", "CREATE"),
    ALTER("DDL", "ALTER"),
    DROP("DDL", "DROP"),
    SELECT("DML", "SELECT"),
    INSERT("DML", "INSERT"),
    UPDATE("DML", "UPDATE"),
    DELETE("DML", "DELETE"),
    TRUNCATE("DML", "TRUNCATE"),
    UNKNOWN("DML", "UNKNOWN");

    private final static String TYPE_QUERY = "SELECT";
    private final static String CATEGORY_DDL = "DDL";
    private final static String CATEGORY_DML = "DML";

    private final String category;
    private final String type;

    private CassandraStatementType(String category, String type) {
        this.category = category;
        this.type = type;
    }

    public String getCategory() {
        return category;
    }

    public String getType() {
        return type;
    }

    public boolean isQuery() {
        return TYPE_QUERY.equals(this.type);
    }

    public boolean isUpdate() {
        return isDML() && !isQuery();
    }

    public boolean isDDL() {
        return CATEGORY_DDL.equals(this.category);
    }

    public boolean isDML() {
        return CATEGORY_DML.equals(this.category);
    }
}
