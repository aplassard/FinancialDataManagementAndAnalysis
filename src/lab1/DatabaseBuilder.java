package lab1;

import java.io.*;
import java.sql.*;
import java.util.HashMap;

public class DatabaseBuilder {
	
	public static boolean isNumber(String s)
	{
		try { 
			Double.parseDouble(s); 
		} catch(NumberFormatException e) { 
			return false; 
		}
		// only got here if we didn't return false
		return true;
	}

	public static boolean isInteger(String s) {
		try { 
			Integer.parseInt(s); 
		} catch(NumberFormatException e) { 
			return false; 
		}
		// only got here if we didn't return false
		return true;
	}

	public static HashMap<String,Integer> loadCompanyHashMap(Connection connection)
	{
		HashMap<String,Integer> company = new HashMap<String,Integer>();
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			ResultSet rs = statement.executeQuery("select * from company_stock");
			String stockName;
			int stockID;
			while(rs.next()){
				stockName = rs.getString("stock_symbol");
				stockID = rs.getInt("id");
				company.put(stockName, stockID);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return company;
	}
	
	public static void loadCompetitorTable(Connection connection, File file)
	{
		BufferedReader br = null;
		String[] lineInfo = null;
		String line = null;
		int id1,id2;
		HashMap<String,Integer> company = loadCompanyHashMap(connection);
		try
		{
			br = new BufferedReader(new FileReader(file));
			Statement statement = connection.createStatement();
			int n = 0;
			while((line=br.readLine())!=null){
				lineInfo = line.split(",");
				id1 = company.get(lineInfo[0]);
				id2 = company.get(lineInfo[1]);
				if(id1!=id2){
					String stat = "insert into competitor values("+(n++)+","+id1+","+id2+")";
					statement.execute(stat);
				}
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadYearlyStockData(Connection connection,File file){
		BufferedReader br = null;
		HashMap<String,Integer> companies = loadCompanyHashMap(connection);
		String stockSymbol;
		String mc2009;
		String mc2010;
		String mc2011;
		String mc2012;
		String mc2013;
		String mc2014;
		String e2009;
		String e2010;
		String e2011;
		String e2012;
		String e2013;
		String e2014;
		try
		{
			Statement statement = connection.createStatement();
			br = new BufferedReader(new FileReader(file));
			String[] lineInfo = null;
			String line = null;
			br.readLine();
			br.readLine();
			String stat;
			int n = 0;
			while((line=br.readLine())!=null)
			{
				lineInfo = line.split(",");
				stockSymbol = lineInfo[0];
				int stockID = companies.get(stockSymbol);
				mc2009 = isNumber(lineInfo[2]) ? lineInfo[2] : "NULL";
				mc2010 = isNumber(lineInfo[3]) ? lineInfo[3] : "NULL";
				mc2011 = isNumber(lineInfo[4]) ? lineInfo[4] : "NULL";
				mc2012 = isNumber(lineInfo[5]) ? lineInfo[5] : "NULL";
				mc2013 = isNumber(lineInfo[6]) ? lineInfo[6] : "NULL";
				mc2014 = "NULL";
				e2009 = isNumber(lineInfo[9])  ? lineInfo[9] : "NULL";
				e2010 = isNumber(lineInfo[10]) ? lineInfo[10] : "NULL";
				e2011 = isNumber(lineInfo[11]) ? lineInfo[11] : "NULL";
				e2012 = isNumber(lineInfo[12]) ? lineInfo[12] : "NULL";
				e2013 = isNumber(lineInfo[13]) ? lineInfo[13] : "NULL";
				e2014 = isNumber(lineInfo[14]) ? lineInfo[14] : "NULL";
//				stat = "insert into yearly_data values("+lineInfo[0]+",\""+lineInfo[1]+"\")";
				stat = "insert into yearly_data values("+n+","+e2009+","+mc2009+",\"2009\","+stockID+")";
				statement.execute(stat);
				n++;
				stat = "insert into yearly_data values("+n+","+e2010+","+mc2010+",\"2010\","+stockID+")";
				statement.execute(stat);
				n++;
				stat = "insert into yearly_data values("+n+","+e2011+","+mc2011+",\"2011\","+stockID+")";
				statement.execute(stat);
				n++;
				stat = "insert into yearly_data values("+n+","+e2012+","+mc2012+",\"2012\","+stockID+")";
				statement.execute(stat);
				n++;
				stat = "insert into yearly_data values("+n+","+e2013+","+mc2013+",\"2013\","+stockID+")";
				statement.execute(stat);
				n++;
				stat = "insert into yearly_data values("+n+","+e2014+","+mc2014+",\"2014\","+stockID+")";
				statement.execute(stat);
				n++;
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void loadSectorTable(Connection connection,File file){
		BufferedReader br=null;
		try 
		{
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

	public static void loadIndustryTable(Connection connection,File file){
		try
		{
			String line = null;
			BufferedReader br = new BufferedReader(new FileReader(file));
			String[] lineInfo;
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			while((line=br.readLine())!=null){
				lineInfo = line.split(",");
				String stat = "insert into industry values("+lineInfo[0]+",\""+lineInfo[1]+"\","+lineInfo[2]+")";
				statement.execute(stat);
			}
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadCompanyTable(Connection connection, File file,File file2){
		try
		{
			String line = null;
			BufferedReader br = new BufferedReader(new FileReader(file2));
			HashMap<String,Integer> company = new HashMap<String,Integer>();
			br.readLine();

			String[] lineInfo;
			while((line=br.readLine())!=null){
				lineInfo = line.split("\t");
				company.put(lineInfo[2],Integer.parseInt(lineInfo[0]));
			}
			br.close();
			br = new BufferedReader(new FileReader(file));
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			int n = 0;
			br.readLine();
			while((line=br.readLine())!=null){
				line = line.replace("\"", "");
				lineInfo = line.split("\t");
				int sect = company.get(lineInfo[0]);
				if(!isInteger(lineInfo[6]))lineInfo[6]="-1";
				String stat = "insert into company_stock values("+n+",\""+lineInfo[0]+"\""+",\""+lineInfo[1]+"\",\""+lineInfo[2]+"\",\""+lineInfo[3]+"\",\""+lineInfo[4]+"\",\""+lineInfo[5]+"\","+lineInfo[6]+","+sect+")";
				statement.execute(stat);
				n++;
			}
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static int loadStockPriceTable(Connection connection, String ticker, String id, int num){
		try
		{
			String line = null;
			String[] lineInfo;
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			String timestamp = " 00:00:00";
			File f = new File("lab1/data/PriceData/"+ticker+".csv");
			BufferedReader br = new BufferedReader(new FileReader(f));
			br.readLine();
			while((line=br.readLine())!=null){
				lineInfo = line.split(",");
				String stat = "insert into stock_price values("+num+","+lineInfo[1]+","+lineInfo[4]+","+lineInfo[2]+","+lineInfo[3]+","+lineInfo[5]+",\""+lineInfo[0]+timestamp+"\","+id+")";
				statement.execute(stat);
				num++;
			}
			System.out.println(ticker);
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num;
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
			statement.execute("create table yearly_data(\"id\" INTEGER PRIMARY KEY NOT NULL,\"earnings\" REAL,\"market_cap\" INTEGER,\"year\" VARCHAR(4) NOT NULL,\"company_stockID\" INTEGER NOT NULL, FOREIGN KEY(\"company_stockID\") REFERENCES company_stock(\"id\"));");
			System.out.println("Successfully created the yearly_data table");
			statement.execute("drop table if exists competitor;");
			statement.execute("create table competitor(\"id\" INTEGER PRIMARY KEY NOT NULL,\"competitorID\" INTEGER NOT NULL,\"competiteeID\" INTEGER NOT NULL,FOREIGN KEY(\"competitorID\") REFERENCES company_stock(\"id\"),FOREIGN KEY(\"competiteeID\") REFERENCES company_stock(\"id\")	);");
			System.out.println("Successfully created the competitor table");
//			statement.execute("drop table if exists stock_price;");
//			statement.execute("create table stock_price(\"id\" INTEGER PRIMARY KEY NOT NULL,\"open\" REAL NOT NULL,\"close\" REAL NOT NULL,\"high\" REAL NOT NULL,\"low\" REAL NOT NULL,\"volume\" INTEGER NOT NULL,\"date\" DATETIME NOT NULL,\"company_stockID\" INTEGER NOT NULL,FOREIGN KEY(\"company_stockID\") REFERENCES company_stock(\"id\"));");
//			System.out.println("Successfully created stock_price table");
			f = new File("lab1/data/sector.csv");
			loadSectorTable(connection,f);
			ResultSet rs = statement.executeQuery("select * from sector");
			while(rs.next()){
				System.out.println("ID: "+rs.getString("id")+" Name: "+rs.getString("name"));
			}
			f = new File("lab1/data/industry_sector_table.csv");
			loadIndustryTable(connection,f);
			rs = statement.executeQuery("select * from industry");
			while(rs.next()){
				System.out.println("ID: "+rs.getString("id")+" Name: "+rs.getString("name")+" SectorID: "+rs.getString("sectorID"));
			}
			f = new File("lab1/data/Company_Profile_Stock_Profile_Tab_Delimited.txt");
			File f2 = new File("lab1/data/company.csv");
			loadCompanyTable(connection,f,f2);
			rs = statement.executeQuery("select * from company_stock");
			while(rs.next()){
				System.out.println("Stock Symbol: "+rs.getString("stock_symbol"));
			}
			rs = statement.executeQuery("select * from company_stock");
/*			int num = 0;
			while(rs.next()) {
				System.out.println(num);
				num = loadStockPriceTable(connection, rs.getString("stock_symbol"), rs.getString("id"), num);
				num++;
			}
*/
			f = new File("lab1/data/Ticker_MarketCap_Earnings.csv");
			loadYearlyStockData(connection,f);
			rs = statement.executeQuery("select * from yearly_data");
			while(rs.next()){
				System.out.println("Stock ID: "+rs.getString("company_stockID")+" Earnings: "+rs.getFloat("earnings")+" Market Cap: "+rs.getInt("market_cap")+" Year: "+rs.getString("year"));
			}
			f = new File("lab1/data/competitors.csv");
			loadCompetitorTable(connection,f);
			rs = statement.executeQuery("select * from competitor");
			while(rs.next()){
				System.out.println("Competitor: "+rs.getInt("competitorID")+" Competitee: "+rs.getInt("competiteeID"));
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
