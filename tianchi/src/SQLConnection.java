

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class SQLConnection {
	private  static String driver = "com.mysql.jdbc.Driver";
	private static String userName = "root";
	private static String password = "";
	private static String url="jdbc:mysql://localhost:3306/tianchi";
	
	public static Connection getConnection() {
		Properties myProp = new Properties();
		myProp.put("user", userName);
		myProp.put("password", password);
		Connection con = null;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(url,userName,password);
			System.out.println("successful!");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}
	
	
	public static void cutDownConnect(Connection conn, ResultSet rs) {
		try {

			if (rs != null)
				rs.close();

			if (conn != null)
				conn.close();

		} catch (Exception e) {

			e.printStackTrace();

		}
	}
	
	public static void downBatchConnection(Connection conn,PreparedStatement pst){
		try {
			if (null != pst) {
				pst.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if (null != conn) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(PreparedStatement stmt) {
		try {
			if (stmt != null)
				stmt.close();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public static void close(ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		for(int i=0;i<10;i++){
			Connection con = SQLConnection.getConnection();
			downBatchConnection( con,null);
		}
		
	}

}
