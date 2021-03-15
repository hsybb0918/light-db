package ed.inf.adbs.lightdb.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

/**
 * Global entity using the singleton pattern, keep track of information
 * such as where a file for a given table is located, what the schema is, etc.
 *
 * ClassName: DBCatalog
 * Date: 12 March, 2021
 * Author: Cyan
 */
public class DBCatalog {
    private static DBCatalog instance; // singleton

    private String dataDirectory; // data directory
    private String schemaPath; // schema file path

    private Map<String, List<String>> tableToSchema; // real table name -> schema list
    private Map<String, String> aliasToTable; // alias -> real table name

    /**
     * Constructor: singleton, make the constructor private.
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
     * Getter: get alias to table mapping.
     *
     * @return alias to table mapping
     */
    public Map<String, String> getAliasToTable() {
        return aliasToTable;
    }

    /**
     * Getter: get table to schema mapping.
     *
     * @return table to schema mapping
     */
    public Map<String, List<String>> getTableToSchema() {
        return tableToSchema;
    }

    /**
     * Generate buffer reader on database file according to table name or alias.
     *
     * @param tableName table name or alias
     * @return buffer reader
     */
    public BufferedReader generateTableBuffer(String tableName) {
        // get actual table name
        String actualTableName = (aliasToTable.containsKey(tableName)) ? aliasToTable.get(tableName) : tableName;
        try {
            // return buffer reader, do not forget .csv
            return new BufferedReader(new FileReader(dataDirectory + actualTableName + ".csv"));
        } catch (FileNotFoundException e) {
            System.err.println("Exception occurred when generating buffer reader on database file.");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Generate single schema according to table name.
     *
     * @param tableName table name or alias
     * @return schema without table prefix
     */
    public List<String> generateSingleSchema(String tableName) {
        // get actual table name
        String actualTableName = (aliasToTable.containsKey(tableName)) ? aliasToTable.get(tableName) : tableName;

        // return single schema
        return tableToSchema.get(actualTableName);
    }

    /**
     * Call this function to initialize the DBCatalog.
     *
     * @param dbDirectory database directory
     */
    public void init(String dbDirectory) {
        // init the variables, use linked hash map to ensure the order
        this.tableToSchema = new LinkedHashMap<>();
        this.aliasToTable = new LinkedHashMap<>();

        // init database directory and the schema file
        this.dataDirectory = dbDirectory + File.separator + "data" + File.separator;
        this.schemaPath = dbDirectory + File.separator + "schema.txt";

        // init tables to corresponding schemas
        initTableToSchema();
    }

    /**
     * Initialise the table to schema mapping.
     */
    private void initTableToSchema() {
        BufferedReader br;
        try {
            // open schema file
            br = new BufferedReader(new FileReader(schemaPath));

            String row;
            while ((row = br.readLine()) != null) {
                String[] tokens = row.split(" ");

                String key = tokens[0]; // table name

                ArrayList<String> columnNames = new ArrayList<String>(); // columns
                for (int i = 1; i < tokens.length; i++) {
                    columnNames.add(tokens[i]);
                }

                // generate the mapping
                tableToSchema.put(key, columnNames);
            }
            br.close();
        } catch (Exception e) {
            System.err.println("Exception occurred when dealing with the schema file.");
            e.printStackTrace();
        }
    }
}
