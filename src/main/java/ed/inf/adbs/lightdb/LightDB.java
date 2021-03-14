package ed.inf.adbs.lightdb;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.tools.DBCatalog;
import ed.inf.adbs.lightdb.tools.QueryInterpreter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Lightweight in-memory database system
 *
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

		try {
			DBCatalog.getInstance().init(databaseDir);
			Map<String, String> aliasToTable = DBCatalog.getInstance().getAliasToTable();
			Map<String, List<String>> tableToSchema = DBCatalog.getInstance().getTableToSchema();

//			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Sailors ORDER BY Sailors.B, Sailors.C;");
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				PlainSelect plainSelect = (PlainSelect) ((Select) statement).getSelectBody();
				QueryInterpreter se = new QueryInterpreter(statement);
				se.output(System.out);

				System.out.println("");
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}

//		parsingExample(inputFile);

	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
//            Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				System.out.println("Select body is " + select.getSelectBody());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
