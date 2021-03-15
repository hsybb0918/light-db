package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.SelectVisitor;
import net.sf.jsqlparser.expression.Expression;

/**
 * Has one child, select the tuple which satisfies the condition.
 *
 * ClassName: SelectOperator
 * Date: 13 March, 2021
 * Author: Cyan
 */
public class SelectOperator extends Operator{
    private Operator child;
    private Expression expression;

    private Tuple tuple;
    private SelectVisitor selectVisitor;

    /**
     * Constructor: init the child operator and condition expression.
     *
     * @param expression condition expression
     * @param child child operator
     */
    public SelectOperator(Expression expression, Operator child) {
        this.child = child;
        this.expression = expression;
    }

    /**
     * Return next tuple if it satisfies the condition.
     *
     * @return tuple satisfies the condition
     */
    @Override
    public Tuple getNextTuple() {
        while ((tuple = child.getNextTuple()) != null) {
            selectVisitor = new SelectVisitor(tuple); // init the select visitor using the tuple
            expression.accept(selectVisitor); // evaluate the tuple

            if (selectVisitor.getTupleEvaluationResult()) { // only return if it satisfies the condition
                return tuple;
            }
        }
        return null;
    }

    /**
     * Use function from child.
     */
    @Override
    public void reset() {
        child.reset();
    }
}
