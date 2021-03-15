package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.models.Tuple;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Visitor for select condition expression.
 *
 * ClassName: SelectVisitor
 * Date: 13 March, 2021
 * Author: Cyan
 */
public class SelectVisitor extends ExpressionDeParser {
    private Tuple tuple; // just need one tuple

    private long value;
    private boolean result;

    /**
     * Constructor: init the tuple to be evaluated.
     *
     * @param tuple tuple object
     */
    public SelectVisitor(Tuple tuple) {
        this.tuple = tuple;
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
     * ONLY HERE IS DIFFERENT FROM JOIN VISITOR.
     * If column, set the value corresponding to the column name.
     *
     * @param tableColumn column
     */
    @Override
    public void visit(Column tableColumn) {
        String column = tableColumn.toString();
        value = tuple.getTupleMap().get(column);
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
