package ed.inf.adbs.lightdb.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keep track of information such as where a file for a given table is located, what the schema is, etc.,
 * a global entity using the singleton pattern.
 *
 * @ClassName: DBCatalog
 * @Date: 12 March, 2021
 * @Author: Cyan
 */
public class DBCatalog {
    private static DBCatalog instance;

    private String dataDirectory;
    private String schemaPath;

    private Map<String, List<String>> tableToSchema;
    private Map<String, String> aliasToTable;

    /**
     * Singleton: make the constructor private.
     */
    private DBCatalog() { }

    /**
     * Get DBCatalog instance, make sure only one instance exists;
     * not thread-safe, otherwise add synchronized.
     *
     * @return DBCatalog instance
     */
    public static DBCatalog getInstance() {
        if (instance == null) {
            instance = new DBCatalog();
        }
        return instance;
    }

    /**
     * Getter:
     *
     * @return
     */
    public Map<String, String> getAliasToTable() {
        return aliasToTable;
    }

    /**
     * Getter
     *
     * @return
     */
    public Map<String, List<String>> getTableToSchema() {
        return tableToSchema;
    }

    public BufferedReader generateTableBuffer(String tableName) {
        String actualTableName = (aliasToTable.containsKey(tableName)) ? aliasToTable.get(tableName) : tableName;
        try {
            return new BufferedReader(new FileReader(dataDirectory + actualTableName + ".csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> generateTableSchema(String tableName) {
        String actualTableName = (aliasToTable.containsKey(tableName)) ? aliasToTable.get(tableName) : tableName;
        return tableToSchema.get(actualTableName);
    }

    public void init(String dbDirectory) {
        // init the variables
        this.tableToSchema = new HashMap<>();
        this.aliasToTable = new HashMap<>();

        // init database directory and the schema file
        this.dataDirectory = dbDirectory + File.separator + "data" + File.separator;
        this.schemaPath = dbDirectory + File.separator + "schema.txt";

        // init tables to corresponding schemas
        initTableToSchema();
    }

    private void initTableToSchema() {
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(schemaPath));
            String row;
            while ((row = br.readLine()) != null) {
                String[] tokens = row.split(" ");
                String key = tokens[0];
                ArrayList<String> columnNames = new ArrayList<String>();
                for (int i = 1; i < tokens.length; i++) {
                    columnNames.add(tokens[i]);
                }
                tableToSchema.put(key, columnNames);
            }
            br.close();
        } catch (Exception e) {
            System.err.println("Exception occurred when processing table schemas");
            e.printStackTrace();
        }
    }


}
