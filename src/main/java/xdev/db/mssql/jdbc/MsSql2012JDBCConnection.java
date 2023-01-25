/*
 * SqlEngine Database Adapter MsSQL 2012 - XAPI SqlEngine Database Adapter for MsSQL 2012
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.mssql.jdbc;


import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import xdev.db.jdbc.JDBCConnection;


public class MsSql2012JDBCConnection extends JDBCConnection<MsSql2012JDBCDataSource, MsSql2012Dbms>
{
	public MsSql2012JDBCConnection(MsSql2012JDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	
	@Override
	public void createTable(String tableName, String primaryKey, Map<String, String> columnMap,
			boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey,"INTEGER"); //$NON-NLS-1$
		}
		StringBuffer createStatement = null;
		
		if(isAutoIncrement)
		{
			createStatement = new StringBuffer(
					"IF NOT EXISTS ( SELECT [name] FROM sys.tables WHERE [name] = '" + tableName + "' ) CREATE TABLE " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							+ primaryKey + " " + columnMap.get(primaryKey) + " IDENTITY NOT NULL,"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		else
		{
			createStatement = new StringBuffer("CREATE TABLE " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " " + columnMap.get(primaryKey) + ","); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		for(String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				createStatement.append(keySet + " " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		
		createStatement.append(" PRIMARY KEY (" + primaryKey + "))"); //$NON-NLS-1$ //$NON-NLS-2$
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement.toString()); //$NON-NLS-1$
		}
		
		Connection connection = super.getConnection();
		Statement statement = connection.createStatement();
		try
		{
			statement.execute(createStatement.toString());
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
}
