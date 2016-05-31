/*
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
 *
 */
package com.github.cassandra.jdbc;

import com.github.cassandra.jdbc.parser.SqlToCqlTranslator;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.pmw.tinylog.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This represents a parsed SQL statement including its parameters.
 *
 * @author Zhichun Wu
 */
public class ParsedSqlStatement {
    private static final String SQL_KEYWORD_ESCAPING = ".\"$1\"$2";
    // FIXME this might ruin values and comments in the SQL
    private static final Pattern SQL_KEYWORDS_PATTERN = Pattern
            .compile("(?i)\\.(select|insert|update|delete|into|from|where|key|alter|drop|create)([>=<\\.,\\s])",
                    Pattern.DOTALL | Pattern.MULTILINE);

    private static final Pattern MAGIC_COMMENT_PATTERN = Pattern.compile("(?i)^--\\s+set\\s+(.*)$", Pattern.MULTILINE);

    private static final Splitter PARAM_SPLITTER = Splitter.on(';').trimResults().omitEmptyStrings();
    private static final Splitter KVP_SPLITTER = Splitter.on('=').trimResults().limit(2);

    private static final String KEY_REPLACE_NULL_VALUE = "replace_null_value";
    private static final String KEY_CONSISTENCY_LEVEL = "consistency_level";
    private static final String KEY_FETCH_SIZE = "fetch_size";
    private static final String KEY_NO_LIMIT = "no_limit";
    private static final String KEY_PARSE_SQL = "parse_sql";
    private static final String KEY_READ_TIMEOUT = "read_timeout";

    public static ParsedSqlStatement parse(String sql) {
        sql = Strings.nullToEmpty(sql).trim();

        boolean ddl = true;
        boolean query = false;

        Map<String, String> attributes = new HashMap<String, String>();
        ParsedSqlStatement sqlStmt = null;

        try {
            // extract attributes from magic comment
            Matcher m = MAGIC_COMMENT_PATTERN.matcher(sql);
            while (m.find()) {
                for (String attr : PARAM_SPLITTER.split(m.group(1))) {
                    List<String> kvp = KVP_SPLITTER.splitToList(attr);
                    if (kvp.size() == 2) {
                        attributes.put(kvp.get(0).toLowerCase(), kvp.get(1));
                    }
                }
            }

            // workaround for limitation of JSqlParser - escaping keyword-like columns
            m = SQL_KEYWORDS_PATTERN.matcher(sql);
            sql = m.replaceAll(SQL_KEYWORD_ESCAPING);

            // go ahead to parse the SQL
            Statement s = CCJSqlParserUtil.parse(sql);

            // now translate the SQL query to CQL
            if (s instanceof Select) {
                ddl = false;
                query = true;

                Select select = (Select) s;
                SqlToCqlTranslator trans = new SqlToCqlTranslator();
                select.getSelectBody().accept(trans);
                sql = select.toString();

                sqlStmt = new ParsedSqlStatement(sql, false, true, attributes);
            } else if (s instanceof Insert || s instanceof Update || s instanceof Delete) {
                ddl = false;
            } else {
                sql = s.toString();
            }
        } catch (Throwable t) {
            Logger.debug("Not able to translate the following SQL to CQL - treat it as CQL\n{}\n", sql);
        }

        Logger.debug("Normalized SQL:\n{}", sql);

        if (sqlStmt == null) {
            sqlStmt = new ParsedSqlStatement(sql, false, false, attributes);
        }

        return sqlStmt;
    }

    private final String sql;
    private final boolean ddl;
    private final boolean query;
    private final Map<String, String> attributes;

    private ParsedSqlStatement(String sql, boolean ddl, boolean query, Map<String, String> attributes) {
        this.sql = Strings.nullToEmpty(sql);
        this.ddl = ddl;
        this.query = query;

        this.attributes = new HashMap<String, String>();
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
    }

    public String getSql() {
        return sql;
    }

    public boolean isDdl() {
        return ddl;
    }

    public boolean isQuery() {
        return query;
    }

    /**
     * Get read timeout(read_timeout) from magic comment.
     *
     * @return read timeout in seconds
     */
    public int getReadTimeout() {
        return 0;
    }

    /**
     * Get consistency level(consistency_level) from magic comment.
     *
     * @return consistency level
     */
    public String getConsistencyLevel() {
        return null;
    }

    public boolean noLimit() {
        return Boolean.TRUE.equals(attributes.get(KEY_NO_LIMIT));
    }

    public boolean replaceNullValue() {
        return Boolean.valueOf(attributes.get(KEY_REPLACE_NULL_VALUE));
    }
}
