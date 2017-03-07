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

import com.github.cassandra.jdbc.cql.SqlToCqlTranslator;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.pmw.tinylog.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;

/**
 * This represents a parsed SQL statement including its parameters.
 *
 * @author Zhichun Wu
 */
public class CassandraCqlParser {
    private static final String SQL_KEYWORD_ESCAPING = ".\"$1\"$2";
    // FIXME this might ruin values and comments in the SQL
    private static final Pattern SQL_KEYWORDS_PATTERN = Pattern
            .compile("(?i)\\.(select|insert|update|delete|into|from|where|key|alter|drop|create)([>=<\\.,\\s])",
                    Pattern.DOTALL | Pattern.MULTILINE);

    private static final Pattern MAGIC_COMMENT_PATTERN = Pattern
            .compile("(?i)^(//|--)\\s+set\\s+(.*)$", Pattern.MULTILINE);
    private static final Pattern CQL_COMMENTS_PATTERN
            = Pattern.compile("(/\\*(.|[\\r\\n])*?\\*/)|(--(.*|[\\r\\n]))|(//(.*|[\\r\\n]))", Pattern.MULTILINE);

    private static final Splitter PARAM_SPLITTER = Splitter.on(';').trimResults().omitEmptyStrings();
    private static final Splitter KVP_SPLITTER = Splitter.on('=').trimResults().limit(2);

    private static final String HINT_ALTER = "Alter";
    private static final String HINT_CREATE = "Create";
    private static final String HINT_DROP = "Drop";

    // FIXME the cache should be bounded to each connection, or better each connectionUrl
    private static final Cache<String, CassandraCqlStatement> STMT_CACHE =
            CacheBuilder.newBuilder().maximumSize(CassandraConfiguration.DEFAULT.getCqlCacheSize()).build();

    private static Map<String, String> parseMagicComments(String sql) {
        Map<String, String> attributes = new HashMap<String, String>();

        // extract attributes from magic comment
        Matcher m = MAGIC_COMMENT_PATTERN.matcher(sql);
        while (m.find()) {
            for (String attr : PARAM_SPLITTER.split(m.group(2))) {
                List<String> kvp = KVP_SPLITTER.splitToList(attr);
                if (kvp.size() == 2) {
                    attributes.put(kvp.get(0).toLowerCase(), kvp.get(1));
                }
            }
        }

        return attributes;
    }

    private static CassandraCqlStatement parseSql(CassandraConfiguration config, String sql, Map<String, String> hints) {
        CassandraStatementType stmtType = CassandraStatementType.UNKNOWN;
        if (Strings.isNullOrEmpty(sql)) {
            return new CassandraCqlStatement(Strings.nullToEmpty(sql),
                    new CassandraCqlStmtConfiguration(config, stmtType, hints));
        }

        CassandraCqlStatement sqlStmt = null;
        CassandraCqlStmtConfiguration stmtConfig = null;
        try {
            // workaround for limitation of JSqlParser - escaping keyword-like columns
            Matcher m = SQL_KEYWORDS_PATTERN.matcher(sql);
            sql = m.replaceAll(SQL_KEYWORD_ESCAPING);

            // go ahead to parse the SQL
            Statement s = CCJSqlParserUtil.parse(sql);

            // now translate the SQL query to CQL
            sql = s.toString();
            if (s instanceof Select) {
                stmtType = CassandraStatementType.SELECT;
            } else if (sql.startsWith(CassandraStatementType.INSERT.getType())) {
                stmtType = CassandraStatementType.INSERT;
            } else if (sql.startsWith(CassandraStatementType.UPDATE.getType())) {
                stmtType = CassandraStatementType.UPDATE;
            } else if (sql.startsWith(CassandraStatementType.DELETE.getType())) {
                stmtType = CassandraStatementType.DELETE;
            } else if (sql.startsWith(CassandraStatementType.TRUNCATE.getType())) {
                stmtType = CassandraStatementType.TRUNCATE;
            } else if (sql.startsWith(CassandraStatementType.CREATE.getType())) {
                stmtType = CassandraStatementType.CREATE;
            } else if (sql.startsWith(CassandraStatementType.ALTER.getType())) {
                stmtType = CassandraStatementType.ALTER;
            } else if (sql.startsWith(CassandraStatementType.DROP.getType())) {
                stmtType = CassandraStatementType.DROP;
            }

            stmtConfig = new CassandraCqlStmtConfiguration(config, stmtType, hints);

            if (stmtType.isQuery()) {
                Select select = (Select) s;
                SqlToCqlTranslator trans = new SqlToCqlTranslator(stmtConfig);
                select.getSelectBody().accept(trans);
                sql = select.toString();
            }
        } catch (Throwable t) {
            Logger.debug("Failed to parse the given SQL, fall back to CQL parser");
            sqlStmt = parseCql(config, sql, hints);
            sql = sqlStmt.getCql();
        }

        if (sqlStmt == null) {
            sqlStmt = new CassandraCqlStatement(sql, stmtConfig == null
                    ? new CassandraCqlStmtConfiguration(config, stmtType, hints) : stmtConfig);
        }

        return sqlStmt;
    }

    private static CassandraCqlStatement parseCql(CassandraConfiguration config, String cql,
                                                  Map<String, String> hints) {
        CassandraStatementType stmtType = CassandraStatementType.UNKNOWN;
        if (Strings.isNullOrEmpty(cql)) {
            return new CassandraCqlStatement(Strings.nullToEmpty(cql),
                    new CassandraCqlStmtConfiguration(config, stmtType, hints));
        }


        CassandraCqlStatement cqlStmt = null;
        CassandraCqlStmtConfiguration stmtConfig = null;
        try {
            // FIXME unfortunately not working all the time...
            Matcher matcher = CQL_COMMENTS_PATTERN.matcher(cql);
            String modifiedCql = matcher.replaceAll(EMPTY_STRING).trim();
            int firstWs = modifiedCql.indexOf(' ');
            if (firstWs > 0) {
                stmtType = Enum.valueOf(CassandraStatementType.class, modifiedCql.substring(0, firstWs).toUpperCase());
            }

            stmtConfig = new CassandraCqlStmtConfiguration(config, stmtType, hints);

            /* until there's better way to do that...
            if (stmtType.isQuery()) {
                // FIXME replace original CQL with the formatted one(e.g. limit has been applied / removed)
                cql = new CqlSelectFormatter().format(stmtConfig, (SelectStatement.RawStatement) stmt);
            }
            */
        } catch (Throwable t) {
            Logger.debug(t, "Not able to parse given CQL - treat it as is\n{}\n", cql);
        }

        if (cqlStmt == null) {
            cqlStmt = new CassandraCqlStatement(cql, stmtConfig == null
                    ? new CassandraCqlStmtConfiguration(config, stmtType, hints) : stmtConfig);
        }

        return cqlStmt;
    }


    public static CassandraCqlStatement parse(final CassandraConfiguration config, final String sql) {
        try {
            CassandraCqlStatement parsedStmt = STMT_CACHE.get(sql, new Callable<CassandraCqlStatement>() {
                public CassandraCqlStatement call() throws Exception {
                    String nonNullSql = Strings.nullToEmpty(sql).trim();

                    Map<String, String> attributes = parseMagicComments(nonNullSql);
                    return config == null || config.isSqlFriendly()
                            ? parseSql(config, nonNullSql, attributes)
                            : parseCql(config, nonNullSql, attributes);
                }
            });

            // Logger.debug("Parsed CQL:\n{}", parsedStmt.getCql());

            return parsedStmt;
        } catch (ExecutionException e) {
            throw CassandraErrors.unexpectedException(e.getCause());
        }
    }
}
