package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;

/**
 * @ClassName: DuplicateEliminationOperator
 * @Date: 14 March, 2021
 * @Author: Cyan
 */
public class DuplicateEliminationOperator extends Operator {
    private Operator child;

    private Tuple lastTuple;
    private Tuple currentTuple;

    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
    }

    @Override
    public Tuple getNextTuple() {
        if (lastTuple == null) {
            lastTuple = child.getNextTuple();
            return lastTuple;
        } else {
            while ((currentTuple = child.getNextTuple()) != null) {
                if (!(currentTuple.getTupleMap().equals(lastTuple.getTupleMap()))) {
                    break;
                }
            }
            lastTuple = currentTuple;
            return currentTuple;
        }
    }

    @Override
    public void reset() {
        child.reset();
    }
}
