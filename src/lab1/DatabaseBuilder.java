package lab1;

import java.sql.*;

public class DatabaseBuilder {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException {

		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");

		Connection connection = null;
		try
		{
			// create a database connection
			connection = DriverManager.getConnection("jdbc:sqlite:lab1.db");
			System.out.println("Successfully created the database");
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			statement.execute("drop table if exists sector;"); // this is all shit code and I hate myself for writing it
			statement.execute("create table sector(\"id\" INTEGER PRIMARY KEY NOT NULL,\"name\" VARCHAR(100));");
			System.out.println("Successfully created the sector table");
			statement.execute("drop table if exists industry;");
			statement.execute("create table industry(\"id\" INTEGER PRIMARY KEY NOT NULL, \"name\" VARCHAR(100), \"sectorID\" INTEGER NOT NULL,FOREIGN KEY(\"sectorID\") REFERENCES sector(\"id\"));");
			System.out.println("Successfully created the industry table");
			statement.execute("drop table if exists company_stock;");
			statement.execute("create table company_stock(	\"id\" INTEGER PRIMARY KEY NOT NULL,\"stock_symbol\" VARCHAR(10) NOT NULL,\"company_name\" VARCHAR(100) NOT NULL,\"address\" VARCHAR(256) NOT NULL,\"phone_number\" VARCHAR(10) NOT NULL,\"website\" VARCHAR(100) NOT NULL,\"index_membership\" VARCHAR(100) NOT NULL,	\"number_of_employees\" INTEGER NOT NULL,\"industryID\" INTEGER NOT NULL,FOREIGN KEY(\"industryID\") REFERENCES industry(\"id\")	);");
			System.out.println("Successfully create the company_stock table");
		}
		catch(SQLException e)
		{
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
		}
		finally
		{
			try
			{
				if(connection != null)
					connection.close();
			}
			catch(SQLException e)
			{
				// connection close failed.
				System.err.println(e);
			}

		}
	}

}
