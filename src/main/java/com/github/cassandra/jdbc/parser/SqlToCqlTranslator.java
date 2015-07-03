package com.github.cassandra.jdbc.parser;

import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_ROW_LIMIT;

import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;

public class SqlToCqlTranslator implements SelectVisitor, FromItemVisitor,
		SelectItemVisitor, ExpressionVisitor {

	public void visit(Addition addition) {
		throw new UnsupportedOperationException("Not supported yet.");
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
		throw new UnsupportedOperationException("Not supported yet.");
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
		if (plainSelect.getInto() != null
				|| plainSelect.getGroupByColumnReferences() != null
				|| plainSelect.getJoins() != null
				|| plainSelect.getFromItem() == null
				|| plainSelect.getSelectItems() == null) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		plainSelect.getFromItem().accept(this);
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

		Limit limit = plainSelect.getLimit();
		if (limit == null) {
			limit = new Limit();
			limit.setRowCount(DEFAULT_ROW_LIMIT);
			plainSelect.setLimit(limit);
		} else {
			// turn off not supported features
			limit.setLimitAll(false);
			limit.setOffsetJdbcParameter(false);
			limit.setOffset(-1);

			// apply default limits
			if (limit.getRowCount() <= 0) {
				limit.setRowCount(DEFAULT_ROW_LIMIT);
			}
		}
	}

	public void visit(RegExpMatchOperator rexpr) {
		throw new UnsupportedOperationException("Not supported yet.");
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

	public void visit(WhenClause whenClause) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void visit(WithItem withItem) {
		throw new UnsupportedOperationException("Not supported yet.");
	}
}
