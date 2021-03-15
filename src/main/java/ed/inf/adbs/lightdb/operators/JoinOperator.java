package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.JoinVisitor;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Has two children, join two tables on condition expression.
 *
 * ClassName: JoinOperator
 * Date: 14 March, 2021
 * Author: Cyan
 */
public class JoinOperator extends Operator{
    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;

    private Tuple leftTuple;
    private Tuple rightTuple;
    private Tuple resultTuple;
    private JoinVisitor joinVisitor;

    /**
     * Constructor: init the variables and get the left and right tuple first.
     *
     * @param joinCondition condition expression on join
     * @param leftChild left child operator
     * @param rightChild right child operator
     */
    public JoinOperator(Expression joinCondition, Operator leftChild, Operator rightChild) {
        this.joinCondition = joinCondition;
        this.leftChild = leftChild;
        this.rightChild = rightChild;

        // need to get the left and right tuple first
        this.leftTuple = leftChild.getNextTuple();
        this.rightTuple = rightChild.getNextTuple();
    }

    /**
     * Used to combine two tuple if they satisfy the join condition expression.
     *
     * @param left left tuple
     * @param right right tuple
     * @return joined tuple
     */
    public Tuple combineTuples(Tuple left, Tuple right) {
        List<String> tupleColumns = new ArrayList<>();
        List<Integer> tupleValues = new ArrayList<>();

        // first iterate the left tuple columns and values
        for (Map.Entry<String, Integer> entry : left.getTupleMap().entrySet()) {
            tupleColumns.add(entry.getKey());
            tupleValues.add(entry.getValue());
        }

        // then iterate the right tuple columns and values
        for (Map.Entry<String, Integer> entry : right.getTupleMap().entrySet()) {
            tupleColumns.add(entry.getKey());
            tupleValues.add(entry.getValue());
        }

        return new Tuple(tupleValues, tupleColumns);
    }

    /**
     * Left deep join tree that follows the order in the from clause.
     *
     * @return tuple object
     */
    @Override
    public Tuple getNextTuple() {
        // IMPORTANT: should clear the result tuple every time call the function
        resultTuple = null;

        // only loop when still having left and right tuples
        while (leftTuple != null && rightTuple != null) {
            if (joinCondition == null) { // if no condition, just combine
                resultTuple = combineTuples(leftTuple, rightTuple);
            }  else { // if have condition, determine whether satisfy
                joinVisitor = new JoinVisitor(leftTuple, rightTuple); // init the join visitor
                joinCondition.accept(joinVisitor); // evaluate the tuple

                if (joinVisitor.getTupleEvaluationResult()) {
                    resultTuple = combineTuples(leftTuple, rightTuple);
                }
            }

            // scan the table on the right and keep the left table fixed
            // reset the right table if have no tuple any more and move left table to next
            if (leftTuple != null) { // as long as the left table not null
                if (rightTuple != null) { // move only on the right
                    rightTuple = rightChild.getNextTuple();
                }
                if (rightTuple == null) { // move on the left and then right
                    leftTuple = leftChild.getNextTuple();

                    rightChild.reset();
                    rightTuple = rightChild.getNextTuple();
                }
            }

            if (resultTuple != null) {
                return resultTuple;
            }
        }
        return null;
    }

    /**
     * Reset both the left and right child.
     */
    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
