package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Iterator API, basic abstract class, all operators will extend it.
 *
 * ClassName: Operator
 * Date: 12 March, 2021
 * Author: Cyan
 */
public abstract class Operator {
    /**
     * Repeatedly get the next tuple of the output.
     *
     * @return next tuple, or null if no next tuple
     */
    public abstract Tuple getNextTuple();

    /**
     * Reset the state and start returning the output from the beginning.
     */
    public abstract void reset();

    /**
     * Output the tuples to a suitable stream, from root.
     *
     * @param ps output stream
     */
    public void dump(PrintStream ps) {
        Tuple tuple = getNextTuple();
        StringBuilder sb = new StringBuilder();

        // if the file is not empty, then read the first tuple and save to string builder
        // else, empty relation gets empty result
        if (tuple != null) {
            sb.append(tuple.getTupleString());

            // continue reading next tuple and add "\n"
            while ((tuple = getNextTuple()) != null) {
                sb.append("\n").append(tuple.getTupleString());
            }
        }

        // after reading all tuples, write to the output stream
        try {
            ps.write(sb.toString().getBytes());
        } catch (IOException e) {
            System.err.println("Exception occurred when writing to the output stream.");
            e.printStackTrace();
        }
    }
}
