package xdev.db.mssql.jdbc;

/*-
 * #%L
 * SqlEngine Database Adapter MsSQL 2012
 * %%
 * Copyright (C) 2003 - 2021 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ObjectUtils;
import xdev.util.ProgressMonitor;


public class MsSql2012JDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID	= -5979795420664514298L;
	
	
	public MsSql2012JDBCMetaData(MsSql2012JDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types)
			throws DBException
	{
		
		return super.getTableInfos(monitor,types);
	}
	
	
	@Override
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table) throws DBException, SQLException
	{
		String catalog = getCatalog(this.dataSource);
		String schema = table.getSchema();
		
		String tableName = table.getName();
		
		Map<String, Object> defaultValues = new HashMap<>();
		ResultSet rs = meta.getColumns(catalog,schema,tableName,null);
		while(rs.next())
		{
			String columnName = rs.getString("COLUMN_NAME");
			Object defaultValue = rs.getObject("COLUMN_DEF");
			defaultValues.put(columnName,defaultValue);
		}
		rs.close();
		
		if(schema != null)
		{
			tableName = table.getSchema() + "." + table.getName();
		}
		
		Table tableIdentity = new Table(tableName,"META_DUMMY");
		
		SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result result = jdbcConnection.query(select);
		int cc = result.getColumnCount();
		ColumnMetaData[] columns = new ColumnMetaData[cc];
		for(int i = 0; i < cc; i++)
		{
			ColumnMetaData column = result.getMetadata(i);
			
			Object defaultValue = column.getDefaultValue();
			if(defaultValue == null && defaultValues.containsKey(column.getName()))
			{
				defaultValue = defaultValues.get(column.getName());
			}
			defaultValue = checkDefaultValue(defaultValue,column);
			
			columns[i] = new ColumnMetaData(table.getName(),column.getName(),column.getCaption(),
					column.getType(),column.getLength(),column.getScale(),defaultValue,
					column.isNullable(),column.isAutoIncrement());
		}
		result.close();
		
		Map<IndexInfo, Set<String>> indexMap = new LinkedHashMap<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			Set<String> primaryKeyColumns = new HashSet<>();
			//FIX for XDEV-2808
			rs = meta.getPrimaryKeys(catalog,schema,table.getName());
			while(rs.next())
			{
				primaryKeyColumns.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
			
			if((flags & INDICES) != 0)
			{
				if(primaryKeyColumns.size() > 0)
				{
					indexMap.put(new IndexInfo("PRIMARY_KEY",IndexType.PRIMARY_KEY),
							primaryKeyColumns);
				}
				//FIX for XDEV-2808
				rs = meta.getIndexInfo(catalog,schema,table.getName(),false,true);
				while(rs.next())
				{
					String indexName = rs.getString("INDEX_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					if(indexName != null && columnName != null
							&& !primaryKeyColumns.contains(columnName))
					{
						boolean unique = !rs.getBoolean("NON_UNIQUE");
						IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
								: IndexType.NORMAL);
						Set<String> columnNames = indexMap.get(info);
						if(columnNames == null)
						{
							columnNames = new HashSet<>();
							indexMap.put(info,columnNames);
						}
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
							tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexMap.keySet())
		{
			Set<String> columnList = indexMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,count);
	}
	
	
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> list = new ArrayList<>();
		
		try
		{
			ConnectionProvider connectionProvider = this.dataSource.getConnectionProvider();
			Connection connection = connectionProvider.getConnection();
			
			try
			{
				DatabaseMetaData meta = connection.getMetaData();
				String catalog = getCatalog(this.dataSource);
				String schema = getSchema(this.dataSource);
				
				ResultSet rs = meta.getProcedures(catalog,schema,null);
				while(rs.next() && !monitor.isCanceled())
				{
					if("sys".equals(rs.getString("PROCEDURE_SCHEM")))
					{
						continue;
					}
					
					String name = rs.getString("PROCEDURE_NAME");
					int i = name.indexOf(';');
					if(i > 0)
					{
						name = name.substring(0,i);
					}
					
					String description = rs.getString("REMARKS");
					
					ReturnTypeFlavor returnTypeFlavor;
					DataType returnType = null;
					int procedureType = rs.getInt("PROCEDURE_TYPE");
					switch(procedureType)
					{
						case DatabaseMetaData.procedureNoResult:
							returnTypeFlavor = ReturnTypeFlavor.VOID;
						break;
						
						default:
							returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
					}
					
					List<Param> params = new ArrayList<>();
					ResultSet rsp = meta.getProcedureColumns(catalog,schema,name,null);
					while(rsp.next())
					{
						DataType dataType = DataType.get(rsp.getInt("DATA_TYPE"));
						String columnName = rsp.getString("COLUMN_NAME");
						switch(rsp.getInt("COLUMN_TYPE"))
						{
							case DatabaseMetaData.procedureColumnReturn:
								returnTypeFlavor = ReturnTypeFlavor.TYPE;
								returnType = dataType;
							break;
							
							case DatabaseMetaData.procedureColumnResult:
								returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
							break;
							
							case DatabaseMetaData.procedureColumnIn:
								params.add(new Param(ParamType.IN,columnName,dataType));
							break;
							
							case DatabaseMetaData.procedureColumnOut:
								params.add(new Param(ParamType.OUT,columnName,dataType));
							break;
							
							case DatabaseMetaData.procedureColumnInOut:
								params.add(new Param(ParamType.IN_OUT,columnName,dataType));
							break;
						}
					}
					rsp.close();
					
					list.add(new StoredProcedure(returnTypeFlavor,returnType,name,description,
							params.toArray(new Param[params.size()])));
				}
			}
			finally
			{
				connection.close();
			}
		}
		catch(SQLException e)
		{
			throw new DBException(this.dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb,params,true);
		}
		
		for(Index index : table.getIndices())
		{
			sb.append(", ");
			appendIndexDefinition(index,sb);
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params,true);
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ALTER COLUMN ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params,false);
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
		
		if(column.isAutoIncrement() && !existing.isAutoIncrement())
		{
			sb = new StringBuilder();
			params = new ArrayList();
			
			sb.append("ALTER TABLE ");
			appendEscapedName(table.getTableInfo().getName(),sb);
			sb.append(" ALTER COLUMN ");
			appendEscapedName(existing.getName(),sb);
			sb.append(" IDENTITY(1,1)");
			
			jdbcConnection.write(sb.toString(),false,params.toArray());
		}
		else
		{
			Object defaultValue = column.getDefaultValue();
			if(!ObjectUtils.equals(defaultValue,existing.getDefaultValue())
					&& !(defaultValue == null && !column.isNullable()))
			{
				sb = new StringBuilder();
				params = new ArrayList();
				
				sb.append("ALTER TABLE ");
				appendEscapedName(table.getTableInfo().getName(),sb);
				sb.append(" ALTER COLUMN ");
				appendEscapedName(existing.getName(),sb);
				
				sb.append(" SET DEFAULT ");
				if(defaultValue == null)
				{
					sb.append("NULL");
				}
				else
				{
					sb.append("?");
					params.add(defaultValue);
				}
				
				jdbcConnection.write(sb.toString(),false,params.toArray());
			}
		}
	}
	

	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case BOOLEAN:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				case BINARY:
				case VARBINARY:
				case CLOB:
				case LONGVARCHAR:
				case BLOB:
				case LONGVARBINARY:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case CHAR:
			case VARCHAR:
			{
				int thisLength = thisColumn.getLength();
				int thatLength = thatColumn.getLength();
				return thatType == DataType.LONGVARCHAR && thatLength <= 8000
						&& thisLength == thatLength;
			}
			
			case CLOB:
			{
				return thatType == DataType.LONGVARCHAR
						&& thisColumn.getLength() == thatColumn.getLength();
			}
			
			case BINARY:
			case VARBINARY:
			{
				int thisLength = thisColumn.getLength();
				int thatLength = thatColumn.getLength();
				return thatType == DataType.LONGVARBINARY && thatLength <= 8000
						&& thisLength == thatLength;
			}
			
			case BLOB:
			{
				return thatType == DataType.LONGVARBINARY
						&& thisColumn.getLength() == thatColumn.getLength();
			}
		}
		
		return null;
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb, List params,
			boolean isNewColumn)
	{
		DataType type = column.getType();
		switch(type)
		{
			case TINYINT:
			case SMALLINT:
			case BIGINT:
			case REAL:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
			{
				sb.append(type.name());
			}
			break;
			
			case INTEGER:
			{
				sb.append("INT");
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case BOOLEAN:
			{
				sb.append("BIT");
			}
			break;
			
			case CHAR:
			case VARCHAR:
			{
				int length = column.getLength();
				if(length > 8000)
				{
					sb.append("TEXT");
				}
				else
				{
					sb.append(type.name());
					sb.append("(");
					sb.append(length);
					sb.append(")");
				}
			}
			break;
			
			case BINARY:
			case VARBINARY:
			{
				int length = column.getLength();
				if(length > 8000)
				{
					sb.append("IMAGE");
				}
				else
				{
					sb.append(type.name());
					sb.append("(");
					sb.append(length);
					sb.append(")");
				}
			}
			break;
			
			case CLOB:
			case LONGVARCHAR:
			{
				int length = column.getLength();
				if(length > 8000)
				{
					sb.append("TEXT");
				}
				else
				{
					sb.append("VARCHAR(");
					sb.append(length);
					sb.append(")");
				}
			}
			break;
			
			case BLOB:
			case LONGVARBINARY:
			{
				int length = column.getLength();
				if(length > 8000)
				{
					sb.append("IMAGE");
				}
				else
				{
					sb.append("VARBINARY(");
					sb.append(length);
					sb.append(")");
				}
			}
			break;
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
		
		if(isNewColumn)
		{
			if(column.isAutoIncrement())
			{
				sb.append(" IDENTITY(1,1)");
			}
			else
			{
				Object defaultValue = column.getDefaultValue();
				if(!(defaultValue == null && !column.isNullable()))
				{
					sb.append(" DEFAULT ");
					if(defaultValue == null)
					{
						sb.append("NULL");
					}
					else
					{
						sb.append("?");
						params.add(defaultValue);
					}
				}
			}
		}
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private void appendIndexDefinition(Index index, StringBuilder sb)
	{
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("CONSTRAINT ");
				appendEscapedName(index.getName(),sb);
				sb.append(" PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("CONSTRAINT ");
				appendEscapedName(index.getName(),sb);
				sb.append(" UNIQUE");
			}
			break;
			
			case NORMAL:
			{
				return;
			}
		}
		
		sb.append(" (");
		String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP CONSTRAINT ");
		appendEscapedName(index.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void appendEscapedName(String name, StringBuilder sb)
	{
		sb.append("'");
		sb.append(name);
		sb.append("'");
	}
}
