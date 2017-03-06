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
package com.github.cassandra.jdbc.cql;

import com.github.cassandra.jdbc.CassandraCqlStmtConfiguration;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.List;


public class SqlToCqlTranslator implements SelectVisitor, FromItemVisitor,
        SelectItemVisitor, ExpressionVisitor {
    private final CassandraCqlStmtConfiguration config;

    public SqlToCqlTranslator(CassandraCqlStmtConfiguration config) {
        this.config = config;
    }

    public void visit(Addition addition) {
        // throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(AllColumns allColumns) {

    }

    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(AllTableColumns allTableColumns) {

    }

    public void visit(AnalyticExpression aexpr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(WithinGroupExpression wgexpr) {

    }

    public void visit(AndExpression andExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Between between) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(BitwiseAnd bitwiseAnd) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(BitwiseOr bitwiseOr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(BitwiseXor bitwiseXor) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(CaseExpression caseExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(CastExpression cast) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Column tableColumn) {
        if (tableColumn.getTable() != null) {
            tableColumn.setTable(null);
        }
    }

    public void visit(Concat concat) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(DateValue dateValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Division division) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(DoubleValue doubleValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(EqualsTo equalsTo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(ExistsExpression existsExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(ExtractExpression eexpr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Function function) {

    }

    public void visit(GreaterThan greaterThan) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(GreaterThanEquals greaterThanEquals) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(InExpression inExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(IntervalExpression iexpr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(IsNullExpression isNullExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(JdbcNamedParameter jdbcNamedParameter) {

    }

    public void visit(JdbcParameter jdbcParameter) {

    }

    public void visit(LateralSubSelect lateralSubSelect) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(LikeExpression likeExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(LongValue longValue) {
        // throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(HexValue hexValue) {

    }

    public void visit(Matches matches) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(MinorThan minorThan) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(MinorThanEquals minorThanEquals) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Modulo modulo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Multiplication multiplication) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(NotEqualsTo notEqualsTo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(NullValue nullValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(OracleHierarchicalExpression oexpr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(OrExpression orExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Parenthesis parenthesis) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getIntoTables() != null
                || plainSelect.getGroupByColumnReferences() != null
                || plainSelect.getJoins() != null
                //|| plainSelect.getFromItem() == null
                || plainSelect.getSelectItems() == null) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        FromItem selectFrom = plainSelect.getFromItem();
        if (selectFrom != null) {
            selectFrom.accept(this);
        }

        List<SelectItem> items = plainSelect.getSelectItems();
        int index = 0;
        for (SelectItem item : items) {
            if (item instanceof AllTableColumns) {
                AllTableColumns atc = (AllTableColumns) item;
                if (atc.getTable() != null) {
                    items.set(index, new AllColumns());
                }
            } else {
                item.accept(this);
            }

            index++;
        }

        long rowLimit = config.getConnectionConfig().getRowLimit();
        if (config.noLimit()) {
            plainSelect.setLimit(null);
        } else if (rowLimit > 0) {
            Limit limit = plainSelect.getLimit();
            if (limit == null) {
                limit = new Limit();
                limit.setRowCount(rowLimit);
                plainSelect.setLimit(limit);
            } else {
                // turn off not supported features
                limit.setLimitAll(false);
                limit.setOffsetJdbcParameter(false);
                limit.setOffset(-1);

                // apply default limits
                if (limit.getRowCount() <= 0) {
                    limit.setRowCount(rowLimit);
                }
            }
        }

    }

    public void visit(RegExpMatchOperator rexpr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(JsonExpression jsonExpr) {

    }

    public void visit(RegExpMySQLOperator regExpMySQLOperator) {

    }

    public void visit(UserVariable var) {

    }

    public void visit(NumericBind bind) {

    }

    public void visit(KeepExpression aexpr) {

    }

    public void visit(MySQLGroupConcat groupConcat) {

    }

    public void visit(RowConstructor rowConstructor) {

    }

    public void visit(OracleHint hint) {

    }

    public void visit(TimeKeyExpression timeKeyExpression) {
    }

    public void visit(DateTimeLiteralExpression literal) {
    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    public void visit(SetOperationList setOpList) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(SignedExpression signedExpression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(StringValue stringValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(SubJoin subjoin) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Subtraction subtraction) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Table tableName) {
        if (tableName.getAlias() != null) {
            tableName.setAlias(null);
        }
    }

    public void visit(TimestampValue timestampValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(TimeValue timeValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(ValuesList valuesList) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(TableFunction tableFunction) {

    }

    public void visit(WhenClause whenClause) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(WithItem withItem) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
