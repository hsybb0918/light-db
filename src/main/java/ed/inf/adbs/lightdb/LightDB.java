package ed.inf.adbs.lightdb;

import java.io.*;

import ed.inf.adbs.lightdb.tools.DBCatalog;
import ed.inf.adbs.lightdb.tools.QueryInterpreter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Lightweight in-memory database system
 */
public class LightDB {
	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		parseQuery(databaseDir, inputFile, outputFile);
	}

	/**
	 * Parse and execute the query.
	 *
	 * @param databaseDir database directory
	 * @param inputFile input file name
	 * @param outputFile output file name
	 */
	public static void parseQuery(String databaseDir, String inputFile, String outputFile) {
		try {
			// first init the database catalog
			DBCatalog.getInstance().init(databaseDir);

			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (statement != null) {
				System.out.println("Read statement: " + statement);

				// init the query interpreter
				QueryInterpreter se = new QueryInterpreter(statement);
				// output the result to file
				se.output(new PrintStream(outputFile));
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing.");
			e.printStackTrace();
		}
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */
	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
            // Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				System.out.println("Select body is " + select.getSelectBody());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing.");
			e.printStackTrace();
		}
	}
}
