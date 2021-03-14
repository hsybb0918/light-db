package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.JoinVisitor;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: JoinOperator
 * @Date: 14 March, 2021
 * @Author: Cyan
 */
public class JoinOperator extends Operator{

    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;

    private Tuple leftTuple;
    private Tuple rightTuple;
    private Tuple resultTuple;
    private JoinVisitor joinVisitor;

    public JoinOperator(Expression joinCondition, Operator leftChild, Operator rightChild) {
        this.joinCondition = joinCondition;
        this.leftChild = leftChild;
        this.rightChild = rightChild;

        this.leftTuple = leftChild.getNextTuple();
        this.rightTuple = rightChild.getNextTuple();

    }

    public Tuple combineTuples(Tuple left, Tuple right) {
        List<String> tupleColumns = new ArrayList<>();
        List<Integer> tupleValues = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : left.getTupleMap().entrySet()) {
            tupleColumns.add(entry.getKey());
            tupleValues.add(entry.getValue());
        }

        for (Map.Entry<String, Integer> entry : right.getTupleMap().entrySet()) {
            tupleColumns.add(entry.getKey());
            tupleValues.add(entry.getValue());
        }

        return new Tuple(tupleValues, tupleColumns);
    }

    @Override
    public Tuple getNextTuple() {
        resultTuple = null;

        while (leftTuple != null && rightTuple != null) {
            if (joinCondition == null) {
                resultTuple = combineTuples(leftTuple, rightTuple);
            }  else {
                joinVisitor = new JoinVisitor(leftTuple, rightTuple);
                joinCondition.accept(joinVisitor); // evaluate the tuple
                if (joinVisitor.getTupleEvaluationResult()) {
                    resultTuple = combineTuples(leftTuple, rightTuple);
                }
            }

            //Now scan the table on the right iteratively by keeping the left table fixed.
            //Reset the table on the right as soon as the end is reached
            if (leftTuple != null) { // Keep checking the RHS table.
                if (rightTuple != null) {
                    rightTuple = rightChild.getNextTuple();
                }
                if (rightTuple == null) {
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

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
