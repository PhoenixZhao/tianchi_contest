import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;


public class BehaviorAnalysis {
	static String testDate = "2014-12-18";

	public static void main(String[] args){
	    HashMap<String,List<String>> user_itemList;
		Connection conn = null;
		PreparedStatement stat = null;
		ResultSet rs = null;
		try {
			conn = SQLConnection.getConnection();
			user_itemList = new HashMap<String,List<String>>();
			String getBuyerBehaviorSQl = "select user_id,item_id from user_behaviors where time like '"+testDate+"%' and behavior_type='4'";
			stat = conn.prepareStatement(getBuyerBehaviorSQl);
			rs = stat.executeQuery();
			List<String> items;
		    while(rs.next()){
		    	if(user_itemList.containsKey(rs.getString("user_id"))){
		    		items = user_itemList.get(rs.getString("user_id"));
		    	}
		    	else{
		    		items = new ArrayList<String>();
		    	}
		    	items.add(rs.getString("item_id"));
	    		user_itemList.put(rs.getString("user_id"), items);
		    }
		    
		    BuyTable(user_itemList);
			SQLConnection.close(stat);
			SQLConnection.close(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void BuyTable(HashMap<String,List<String>> user_itemList){
		PrintWriter pw;
		try {
			pw = new PrintWriter(new File("./data/Buy_User_Behavior_table.txt"));
			Iterator<Entry<String, List<String>>> it = user_itemList.entrySet().iterator();
			while(it.hasNext()){
				Entry<String, List<String>> en = it.next();
				String user = en.getKey();
				List<String> items = en.getValue();
				pw.print(user+"\t");
				for(String s:items){
					pw.print(s+"\t");
				}
				pw.println();
			}
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void BuyPathTable(HashMap<String,List<String>> user_itemList){
		Connection conn = null;
		PreparedStatement stat = null;
		ResultSet rs = null;
		PrintWriter pw;
		String BuyerBehaviorPathSQL = "select behavior_type,time from user_behaviors where user_id=? and item_id=? order by time";
		try {
			conn = SQLConnection.getConnection();
			pw = new PrintWriter(new File("./data/Buy_User_Behavior_path_table.txt"));
			Iterator<Entry<String, List<String>>> it = user_itemList.entrySet().iterator();
			while(it.hasNext()){
				Entry<String, List<String>> en = it.next();
				String user = en.getKey();
				List<String> items = en.getValue();
				pw.print(user+"\t");
				for(String s:items){
					pw.print(s+"\t");
					stat = conn.prepareStatement(BuyerBehaviorPathSQL);
					stat.setString(1, user);
					stat.setString(2, s);
					rs = stat.executeQuery();
					while(rs.next()){
						pw.print(rs.getString("behavior_type")+"\t");
					}
				}
				pw.println();
			}
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
