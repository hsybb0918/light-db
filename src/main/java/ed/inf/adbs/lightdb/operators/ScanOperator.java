package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.DBCatalog;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Scan operator is called upon initialisation of the query.
 *
 * @ClassName: ScanOperator
 * @Date: 12 March, 2021
 * @Author: Cyan
 */
public class ScanOperator extends Operator{

    private String tableName;
    private List<String> singleSchema;
    private BufferedReader tableBuffer;

    private List<String> tableSchema;

    public ScanOperator(String tableName, List<String> singleSchema, BufferedReader tableBuffer) {
        this.tableName = tableName;
        this.singleSchema = singleSchema;
        this.tableBuffer = tableBuffer;

        for (String field : singleSchema) {
            this.tableSchema = new ArrayList<>();
            this.tableSchema.add(tableName + '.' + field);
        }
    }

    @Override
    public List<String> getSchema() {
        return tableSchema;
    }

    @Override
    public Tuple getNextTuple() {
        String row = null;
        try {
            row = tableBuffer.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (row == null) {
            return null;
        }

        String[] rowValues = row.split(",");
        List<Integer> tupleValues = new ArrayList<>();
        for (int i = 0; i < rowValues.length; i++) {
            tupleValues.add(Integer.parseInt(rowValues[i]));
        }

        Tuple tup = new Tuple(tupleValues, singleSchema, tableName);
        return tup;
    }

    @Override
    public void reset() {
        try {
            tableBuffer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        tableBuffer = DBCatalog.getInstance().generateTableBuffer(tableName);
    }
}
