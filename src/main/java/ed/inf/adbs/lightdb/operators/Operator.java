package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Iterator API, basic abstract class, all operators will extend it.
 *
 * @ClassName: Operator
 * @Date: 12 March, 2021
 * @Author: Cyan
 */
public abstract class Operator {

    /**
     * Repeatedly get the next tuple of the output.
     *
     * @return next tuple, or null
     */
    public abstract Tuple getNextTuple();

    /**
     * Reset the state and start returning the output from the beginning.
     */
    public abstract void reset();

    /**
     * Output the tuples to a suitable stream.
     *
     * @param ps output stream
     */
    public void dump(PrintStream ps) {
        Tuple tuple = getNextTuple(); // TODO: the first tuple maybe null tuple

        StringBuilder sb = new StringBuilder();
        sb.append(tuple.getTupleString());

        while ((tuple = getNextTuple()) != null) {
            sb.append("\n").append(tuple.getTupleString());
        }

        try {
            ps.write(sb.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
