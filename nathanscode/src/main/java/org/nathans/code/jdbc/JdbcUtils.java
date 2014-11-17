package org.nathans.code.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class JdbcUtils {
	
	/**
	 * Close a ResultSet catching any exceptions
	 * @param java.sql.ResultSet
	 */
	public static void closeResultSetQuietly(ResultSet rs){
		try{
			if(null != rs)
				rs.close();
		}catch(Exception e){}
	}
	
	/**
	 * Close a PreparedStatement catching any exceptions
	 * @param java.sql.PreparedStatement
	 */
	public static void closePreparedStatementQuietly(PreparedStatement ps){
		try{
			if(null != ps)
				ps.close();
		}catch(Exception e){}
	}
	
	/**
	 * Close a Connection catching any exceptions
	 * @param java.sql.Connection
	 */
	public static void closeConnectionQuietly(Connection conn){
		try {
			closeConnection(conn, false, true);
		} catch (SQLException e) {}
	}
	
	/**
	 * Close a Connection
	 * @param java.sql.Connection conn - Connection to close
	 * @param boolean commitFirst - commit before closing. Note that if auto-commit is set to true, this will throw an exception
	 * @param boolean quietly - re-throw any exceptions or not
	 * @throws SQLException
	 */
	public static void closeConnection(Connection conn, boolean commitFirst, boolean quietly) throws SQLException{
		if(null != conn){
			try{
			if(commitFirst)
				conn.commit();
			conn.close();
			}catch(SQLException e){
				if(!quietly)
					throw e;
			}
		}		
	}
	
	/**
	 * find distinct values of a string / varchar field
	 * @param java.sql.Connection conn
	 * @param java.lang.String table - database table name
	 * @param java.lang.String field - field to distinct on
	 * @param java.lang.String whereClause - where clause to use or null
	 * @return
	 * @throws SQLException
	 */
	public static List<String> distinctStringField(Connection conn, String table, String field, String whereClause) throws SQLException{
		
		StringBuilder query = new StringBuilder("SELECT DISTINCT ").append(field).append(" FROM ").append(table);
		if(StringUtils.isNotBlank(whereClause))
			query.append(" ").append(whereClause);
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		List<String> results = new ArrayList<String>();
		
		try{
			
			ps = conn.prepareStatement(query.toString());
			
			rs = ps.executeQuery();
			
			while(rs.next()){
				results.add(rs.getString(field));
			}
			
			return results;
			
		}catch(SQLException e){
			throw e;
		}finally{
			closeConnectionQuietly(conn);
			closePreparedStatementQuietly(ps);
			closeResultSetQuietly(rs);
		}

		
	}

}
