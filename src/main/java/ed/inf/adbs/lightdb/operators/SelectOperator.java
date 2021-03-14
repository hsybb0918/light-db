package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.SelectVisitor;
import net.sf.jsqlparser.expression.Expression;

import java.util.List;

/**
 * @ClassName: SelectOperator
 * @Date: 13 March, 2021
 * @Author: Cyan
 */
public class SelectOperator extends Operator{
    private Operator child;
    private Expression expression;

    private Tuple tuple;
    private SelectVisitor selectVisitor;

    public SelectOperator(Expression expression, Operator child) {
        this.child = child;
        this.expression = expression;
    }

    @Override
    public Tuple getNextTuple() {
        while ((tuple = child.getNextTuple()) != null) {
            selectVisitor = new SelectVisitor(tuple);
            expression.accept(selectVisitor); // evaluate the tuple
            if (selectVisitor.getTupleEvaluationResult()) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        child.reset();
    }

}
