package ed.inf.adbs.lightdb.tools;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Visitor on the expression with left and right constants.
 *
 * ClassName: ConstantVisitor
 * Date: 15 March, 2021
 * Author: Cyan
 */
public class ConstantVisitor extends ExpressionDeParser {
    private long value;
    private boolean result;

    /**
     * Constructor: nothing init.
     */
    public ConstantVisitor() { }

    /**
     * Get the evaluation result.
     *
     * @return boolean result
     */
    public boolean getTupleEvaluationResult() {
        return result;
    }

    /**
     * CONSTANT VISITOR HAVE NO VISIT METHOD ON COLUMN.
     * If long value, just set value.
     *
     * @param longValue long value
     */
    @Override
    public void visit(LongValue longValue) {
        value = longValue.getValue();
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
