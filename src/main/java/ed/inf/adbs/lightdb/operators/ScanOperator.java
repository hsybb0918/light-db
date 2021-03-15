package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.DBCatalog;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Scan operator is called upon initialisation of the database file.
 *
 * ClassName: ScanOperator
 * Date: 12 March, 2021
 * Author: Cyan
 */
public class ScanOperator extends Operator{
    private String tableName; // actual table name if no alias, may be alias if have alias
    private BufferedReader tableBuffer; // read the database file line by line
    private List<String> tableSchema; // schema with the table name as prefix -> Table.Column

    /**
     * Constructor: init the table schema.
     *
     * @param tableName table name or alias if have
     * @param singleSchema schema without table name as prefix
     * @param tableBuffer read lines
     */
    public ScanOperator(String tableName, List<String> singleSchema, BufferedReader tableBuffer) {
        this.tableName = tableName;
        this.tableBuffer = tableBuffer;

        // combine the table schema with table name as prefix
        this.tableSchema = new ArrayList<>();
        for (String column : singleSchema) {
            this.tableSchema.add(tableName + '.' + column);
        }
    }

    /**
     * Read next line from the database file and new a tuple object.
     *
     * @return tuple object
     */
    @Override
    public Tuple getNextTuple() {
        String row = null;
        try {
            row = tableBuffer.readLine();
        } catch (IOException e) {
            System.err.println("Exception occurred when reading next line from database file.");
            e.printStackTrace();
        }
        if (row == null) { // finish reading
            return null;
        }

        // if get the line, get the values
        String[] rowValues = row.split(",");
        List<Integer> tupleValues = new ArrayList<>();
        for (String rowValue : rowValues) {
            tupleValues.add(Integer.parseInt(rowValue));
        }

        // return new tuple object
        return new Tuple(tupleValues, tableSchema);
    }

    /**
     * Reset the buffer reader.
     */
    @Override
    public void reset() {
        try {
            tableBuffer.close();
        } catch (IOException e) {
            System.err.println("Exception occurred when closing the buffer stream.");
            e.printStackTrace();
        }

        // get a new buffer reader
        tableBuffer = DBCatalog.getInstance().generateTableBuffer(tableName);
    }
}
