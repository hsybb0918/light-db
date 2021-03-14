package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.models.Tuple;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * @ClassName: JoinVisitor
 * @Date: 14 March, 2021
 * @Author: Cyan
 */
public class JoinVisitor extends ExpressionDeParser {
    private Tuple leftTuple;
    private Tuple rightTuple;

    private long value;
    private boolean result;

    public JoinVisitor(Tuple leftTuple, Tuple rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
    }

    public boolean getTupleEvaluationResult() {
        return result;
    }


    @Override
    public void visit(LongValue longValue) {
        value = longValue.getValue();
    }

    @Override
    public void visit(Column tableColumn) {
        String column = tableColumn.toString();
        value = (leftTuple.getTupleMap().get(column) == null) ? rightTuple.getTupleMap().get(column) : leftTuple.getTupleMap().get(column);
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = result;
        andExpression.getRightExpression().accept(this);
        boolean rightResult = result;
        result = leftResult && rightResult;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        long leftResult = value;
        equalsTo.getRightExpression().accept(this);
        long rightResult = value;
        if (leftResult == rightResult) {
            result = true;
        } else {
            result = false;
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        long leftResult = value;
        greaterThan.getRightExpression().accept(this);
        long rightResult = value;
        if (leftResult > rightResult) {
            result = true;
        } else {
            result = false;
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        long leftResult = value;
        greaterThanEquals.getRightExpression().accept(this);
        long rightResult = value;
        if (leftResult >= rightResult) {
            result = true;
        } else {
            result = false;
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        long leftResult = value;
        minorThan.getRightExpression().accept(this);
        long rightResult = value;
        if (leftResult < rightResult) {
            result = true;
        } else {
            result = false;
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        long leftResult = value;
        minorThanEquals.getRightExpression().accept(this);
        long rightResult = value;
        if (leftResult <= rightResult) {
            result = true;
        } else {
            result = false;
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        long leftResult = value;
        notEqualsTo.getRightExpression().accept(this);
        long rightResult = value;
        if (leftResult != rightResult) {
            result = true;
        } else {
            result = false;
        }
    }
}
