package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.models.Tuple;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Visitor for join condition expression.
 *
 * ClassName: JoinVisitor
 * Date: 14 March, 2021
 * Author: Cyan
 */
public class JoinVisitor extends ExpressionDeParser {
    private Tuple leftTuple;
    private Tuple rightTuple;

    private long value;
    private boolean result;

    /**
     * Constructor: init left and right tuple.
     *
     * @param leftTuple left tuple object
     * @param rightTuple right tuple object
     */
    public JoinVisitor(Tuple leftTuple, Tuple rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
    }

    /**
     * Get the evaluation result.
     *
     * @return boolean result
     */
    public boolean getTupleEvaluationResult() {
        return result;
    }

    /**
     * If long value, just set value.
     *
     * @param longValue long value
     */
    @Override
    public void visit(LongValue longValue) {
        value = longValue.getValue();
    }

    /**
     * ONLY HERE IS DIFFERENT FROM SELECT VISITOR.
     * If column, set the value corresponding to the column name of left or right tuple.
     * Because the schema is prefixed with table, there will not have the same schema name.
     *
     * @param tableColumn column
     */
    @Override
    public void visit(Column tableColumn) {
        String column = tableColumn.toString();
        value = (leftTuple.getTupleMap().get(column) == null) ? rightTuple.getTupleMap().get(column) : leftTuple.getTupleMap().get(column);
    }

    /**
     * Process and expression, only true if left and right are all true.
     *
     * @param andExpression and expression
     */
    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = result;
        andExpression.getRightExpression().accept(this);
        boolean rightResult = result;
        result = leftResult && rightResult;
    }

    /**
     * If left value and right value the same, then result is true.
     *
     * @param equalsTo = operator
     */
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

    /**
     * If left value and right value not same, then the result is true.
     *
     * @param notEqualsTo != operator
     */
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

    /**
     * If left is greater than right, then the result is true.
     *
     * @param greaterThan > operator
     */
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

    /**
     * If left is greater or equal to right, then the result is true.
     *
     * @param greaterThanEquals >= operator
     */
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

    /**
     * If left is minor than right, then the result is true.
     *
     * @param minorThan < operator
     */
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

    /**
     * If left is minor or equal to right, then the result is true.
     *
     * @param minorThanEquals <= operator
     */
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
}
