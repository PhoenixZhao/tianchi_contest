import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class SQLUtil {
	static String dataPath = "D:/ShuKai/data/tianchi_mobile_recommend_train_user_original.csv";
	static String insertSQl = "INSERT INTO user_behaviors(user_id,item_id,behavior_type,user_geohash,item_category,time) values(?,?,?,?,?,?)";
	
	public static void main(String[] args){
		Connection conn = null;
		BufferedReader br;
		try {
			 br = new BufferedReader(new InputStreamReader(new FileInputStream(dataPath)));
			 conn = SQLConnection.getConnection();
			 PreparedStatement pst = conn.prepareStatement(insertSQl);
			 String line="";
			 int lineNum=1;
			 while((line=br.readLine())!=null){
				 if(line.contains("user_id")) continue;
				 String[] strs = line.split(",");
				 pst.setString(1, strs[0]);
				 pst.setString(2, strs[1]);
				 pst.setString(3, strs[2]);
				 pst.setString(4, strs[3]);
				 pst.setString(5, strs[4]);
				 pst.setString(6, strs[5]);
				 pst.addBatch();
				 lineNum++;
				 if(lineNum%5000==0){
					 pst.executeBatch();
					 System.out.println("Insert 5000 line successfully!");
				 }
			 }
		    SQLConnection.downBatchConnection(conn, pst);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
} 
