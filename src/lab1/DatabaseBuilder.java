package lab1;

import java.io.*;
import java.sql.*;

public class DatabaseBuilder {
	
	public static void loadSectorTable(Connection connection,File file){
		BufferedReader br=null;
		try {
			String line=null;
			br = new BufferedReader(new FileReader(file));
			String[] lineInfo;
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			while((line=br.readLine())!=null){
				lineInfo = line.split(",");
				String stat = "insert into sector values("+lineInfo[0]+",\""+lineInfo[1]+"\")";
				statement.execute(stat);
			}
			br.close();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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
			File f=null;
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
			statement.execute("drop table if exists yearly_data;");
			statement.execute("create table yearly_data(\"id\" INTEGER PRIMARY KEY NOT NULL,\"earnings\" REAL NOT NULL,\"market_cap\" INTEGER NOT NULL,\"year\" VARCHAR(4) NOT NULL,\"company_stockID\" INTEGER NOT NULL, FOREIGN KEY(\"company_stockID\") REFERENCES company_stock(\"id\"));");
			System.out.println("Successfully created the yearly_data table");
			statement.execute("drop table if exists competitor;");
			statement.execute("create table competitor(\"id\" INTEGER PRIMARY KEY NOT NULL,\"competitorID\" INTEGER NOT NULL,\"competiteeID\" INTEGER NOT NULL,FOREIGN KEY(\"competitorID\") REFERENCES company_stock(\"id\"),FOREIGN KEY(\"competiteeID\") REFERENCES company_stock(\"id\")	);");
			System.out.println("Successfully created the competitor table");
			statement.execute("drop table if exists stock_price;");
			statement.execute("create table stock_price(\"id\" INTEGER PRIMARY KEY NOT NULL,\"open\" REAL NOT NULL,\"close\" REAL NOT NULL,\"high\" REAL NOT NULL,\"low\" REAL NOT NULL,\"volume\" INTEGER NOT NULL,\"date\" DATETIME NOT NULL,\"company_stockID\" INTEGER NOT NULL,FOREIGN KEY(\"company_stockID\") REFERENCES company_stock(\"id\"));");
			System.out.println("Successfully created stock_price table");
			f = new File("lab1/data/sector.csv");
			loadSectorTable(connection,f);
			ResultSet rs = statement.executeQuery("select * from sector");
			while(rs.next()){
				System.out.println("ID: "+rs.getString("id")+" Name: "+rs.getString("name"));
			}
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
