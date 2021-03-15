package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;

import java.util.HashSet;
import java.util.Set;

/**
 * Has one child, eliminate the duplicate tuples.
 * Consider that the output may only be sorted on some limited columns, so here not assumed
 * the output has been sorted, just use a hash set to check if current tuple exists.
 *
 * @ClassName: DuplicateEliminationOperator
 * @Date: 14 March, 2021
 * @Author: Cyan
 */
public class DuplicateEliminationOperator extends Operator {
    private Operator child;

    private Set<Tuple> onceTuples;
    private Tuple tuple;

    /**
     * Constructor: init the child operator
     *
     * @param child child operator
     */
    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        this.onceTuples = new HashSet<>();
    }

    /**
     * If the tuple map is contained in hash set, return true; else return false.
     *
     * @param tuple tuple object
     * @return true if contains in hash set
     */
    public boolean containTuple(Tuple tuple) {
        for (Tuple once : onceTuples) {
            if (once.getTupleMap().equals(tuple.getTupleMap())) {
                return true;
            }
        }
        return false;
    }

    /**
     * If the next tuple is not in the hash set, then add it and return.
     *
     * @return non-duplicated tuple
     */
    @Override
    public Tuple getNextTuple() {
        while ((tuple = child.getNextTuple()) != null) {
            if (onceTuples.isEmpty()) { // if hash set is empty, no need to judge
                onceTuples.add(tuple);
                return tuple;
            }

            if (!containTuple(tuple)) { // if have not appeared in hash set, add and return
                onceTuples.add(tuple);
                return tuple;
            }
        }
        return null;
    }

    /**
     * Use child reset function.
     */
    @Override
    public void reset() {
        child.reset();
    }
}
