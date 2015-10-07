/**
 *
 */
package com.odbc.dao;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.types.Types;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;

public class ODBCConnector {

	public static StringBuilder FOLDER_PATH = new StringBuilder("C:/temp/db/access/");
	public static StringBuilder DB_FILE = new StringBuilder(FOLDER_PATH.toString()).append("db.accdb");

	private static Database createDatabase(final String databaseName) throws IOException {
		return DatabaseBuilder.create(Database.FileFormat.V2003, new File(databaseName));
	}

	private static Database openDatabase(final String databaseName) throws IOException {
		return DatabaseBuilder.open(new File(databaseName));
	}

	private static TableBuilder createTable(final String tableName) {
		return new TableBuilder(tableName).setPrimaryKey("Emp_Id");
	}

	public static void addColumn(final Database database, final TableBuilder tableName, final String columnName,
			final Types sqlType) throws SQLException, IOException {
		tableName.addColumn(new ColumnBuilder(columnName).setSQLType(Types.INTEGER).toColumn()).toTable(database);
	}

	public static void main(final String[] args) {
		try {
			new File(FOLDER_PATH.toString()).mkdirs();
			final File file = new File(DB_FILE.toString());
			if (file.createNewFile()) {
				System.out.println("File is created!");
			} else {
				System.out.println("File already exists.");
			}
			final String filePath = file.getPath().replace("\\", "/");

			final Database database = getDB(filePath);
			Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
			final StringBuilder connectionURL = new StringBuilder("jdbc:ucanaccess://").append(filePath);
			final Connection conn = DriverManager.getConnection(connectionURL.toString());
			final Statement s = conn.createStatement();

			final String tableName = "EmpTable";

			if (!isTableAvailable(conn, tableName)) {
				// Creating table
				final Table table = createTable(tableName)
						.addColumn(new ColumnBuilder("Emp_Id").setSQLType(Types.INTEGER).toColumn())
						.addColumn(new ColumnBuilder("Emp_Name").setSQLType(Types.VARCHAR).toColumn())
						.addColumn(new ColumnBuilder("Emp_Employer").setSQLType(Types.VARCHAR).toColumn())
						.toTable(database);

				table.addRow((Math.random() * 1000.0), "Sarath Kumar Sivan", "Infosys Limited.");//Inserting values into the table
			} else {

				database.getTable(tableName).addRow((Math.random() * 1000.0), "Sarath Kumar Sivan", "Infosys Limited.");
			}
			// create a table
			/*
			 * final String tableName = "myTable" + String.valueOf((int) (Math.random() * 1000.0));
			 * final String createTable = "CREATE TABLE " + tableName +
			 * " (id Integer, name Text(32))";
			 * s.execute(createTable);
			 */

			// enter value into table

			// Fetch table
			final String selTable = "SELECT * FROM " + tableName;
			s.execute(selTable);
			final ResultSet rs = s.getResultSet();
			while ((rs != null) && (rs.next())) {
				System.out.println(rs.getString(1) + " : " + rs.getString(2) + " : " + rs.getString(3));
			}

			// drop the table
			// final String dropTable = "DROP TABLE " + tableName;
			// s.execute(dropTable);

			// close and cleanup
			s.close();
			conn.close();
		} catch (final Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * @param conn
	 * @param tableName
	 * @throws SQLException
	 */
	private static boolean isTableAvailable(final Connection conn, final String tableName) throws SQLException {
		final DatabaseMetaData dbm = conn.getMetaData();
		final ResultSet resultSet = dbm.getTables(null, null, tableName, null);
		while ((resultSet != null) && (resultSet.next())) {
			final String rsTableName = resultSet.getString("TABLE_NAME");
			if (rsTableName.compareToIgnoreCase(tableName) == 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	private static Database getDB(final String filePath) throws IOException {
		Database database = null;
		try {
			database = openDatabase(filePath);
			System.out.println("DB Open ::::");
		} catch (final IOException e) {
			database = createDatabase(filePath);
			System.out.println("DB Created ::::");
		}
		return database;
	}
}
