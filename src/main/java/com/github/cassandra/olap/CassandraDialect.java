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
package com.github.cassandra.olap;

import mondrian.rolap.SqlStatement;
import mondrian.spi.Dialect;
import mondrian.spi.StatisticsProvider;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class CassandraDialect implements Dialect {

    @Override
    public String toUpper(String s) {
        return null;
    }

    @Override
    public String caseWhenElse(String s, String s1, String s2) {
        return null;
    }

    @Override
    public String quoteIdentifier(String s) {
        return null;
    }

    @Override
    public void quoteIdentifier(String s, StringBuilder stringBuilder) {

    }

    @Override
    public String quoteIdentifier(String s, String s1) {
        return null;
    }

    @Override
    public void quoteIdentifier(StringBuilder stringBuilder, String... strings) {

    }

    @Override
    public String getQuoteIdentifierString() {
        return null;
    }

    @Override
    public void quoteStringLiteral(StringBuilder stringBuilder, String s) {

    }

    @Override
    public void quoteNumericLiteral(StringBuilder stringBuilder, String s) {

    }

    @Override
    public void quoteBooleanLiteral(StringBuilder stringBuilder, String s) {

    }

    @Override
    public void quoteDateLiteral(StringBuilder stringBuilder, String s) {

    }

    @Override
    public void quoteTimeLiteral(StringBuilder stringBuilder, String s) {

    }

    @Override
    public void quoteTimestampLiteral(StringBuilder stringBuilder, String s) {

    }

    @Override
    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean allowsAs() {
        return false;
    }

    @Override
    public boolean allowsFromQuery() {
        return false;
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsMultipleCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsMultipleDistinctSqlMeasures() {
        return false;
    }

    @Override
    public boolean allowsCountDistinctWithOtherAggs() {
        return false;
    }

    @Override
    public String generateInline(List<String> list, List<String> list1, List<String[]> list2) {
        return null;
    }

    @Override
    public boolean needsExponent(Object o, String s) {
        return false;
    }

    @Override
    public void quote(StringBuilder stringBuilder, Object o, Datatype datatype) {

    }

    @Override
    public boolean allowsDdl() {
        return false;
    }

    @Override
    public String generateOrderItem(String s, boolean b, boolean b1, boolean b2) {
        return null;
    }

    @Override
    public boolean supportsGroupByExpressions() {
        return false;
    }

    @Override
    public boolean supportsGroupingSets() {
        return false;
    }

    @Override
    public boolean supportsUnlimitedValueList() {
        return false;
    }

    @Override
    public boolean requiresGroupByAlias() {
        return false;
    }

    @Override
    public boolean requiresOrderByAlias() {
        return false;
    }

    @Override
    public boolean requiresHavingAlias() {
        return false;
    }

    @Override
    public boolean allowsOrderByAlias() {
        return false;
    }

    @Override
    public boolean requiresUnionOrderByOrdinal() {
        return false;
    }

    @Override
    public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return false;
    }

    @Override
    public boolean supportsMultiValueInExpr() {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int i, int i1) {
        return false;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return null;
    }

    @Override
    public void appendHintsAfterFromClause(StringBuilder stringBuilder, Map<String, String> map) {

    }

    @Override
    public boolean allowsDialectSharing() {
        return true;
    }

    @Override
    public boolean allowsSelectNotInGroupBy() {
        return false;
    }

    @Override
    public boolean allowsJoinOn() {
        return false;
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return false;
    }

    @Override
    public String generateCountExpression(String s) {
        return null;
    }

    @Override
    public String generateRegularExpression(String s, String s1) {
        return null;
    }

    @Override
    public List<StatisticsProvider> getStatisticsProviders() {
        return null;
    }

    @Override
    public SqlStatement.Type getType(ResultSetMetaData resultSetMetaData, int i) throws SQLException {
        return null;
    }
}
