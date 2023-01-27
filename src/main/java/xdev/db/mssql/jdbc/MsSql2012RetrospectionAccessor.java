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


import static com.xdev.jadoth.sqlengine.SQL.LANG.ASC;
import static com.xdev.jadoth.sqlengine.SQL.LANG.FROM;
import static com.xdev.jadoth.sqlengine.SQL.LANG.ORDER_BY;
import static com.xdev.jadoth.sqlengine.SQL.LANG.SELECT;
import static com.xdev.jadoth.sqlengine.SQL.LANG.WHERE;
import static com.xdev.jadoth.sqlengine.SQL.LANG.__AND;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.NEW_LINE;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._eq_;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.apo;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.comma;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;

import java.sql.ResultSet;

import com.xdev.jadoth.sqlengine.SQL.INDEXTYPE;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardRetrospectionAccessor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.interfaces.SqlExecutor;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlPrimaryKey;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.util.ResultTable;



/**
 * The Class MsSql2012RetrospectionAccessor.
 */
public class MsSql2012RetrospectionAccessor extends StandardRetrospectionAccessor<MsSql2012Dbms>
{
	
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	/** The Constant Column_index_name. */
	public static final String	Column_index_name			= "index_name";
	
	/** The Constant Column_index_description. */
	public static final String	Column_index_description	= "index_description";
	
	/** The Constant Column_index_keys. */
	public static final String	Column_index_keys			= "index_keys";
	
	/** The Constant SqlProc_EXEC_sp_helpindex. */
	public static final String	SqlProc_EXEC_sp_helpindex	= "EXEC sp_helpindex";
	
	// /////////////////////////////////////////////////////////////////////////
	// static fields //
	// ///////////////////
	/** The Constant __. */
	private static final String	__							= _ + "" + _;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new ms sql2005 retrospection accessor.
	 * 
	 * @param dbmsadaptor
	 *            the dbmsadaptor
	 */
	public MsSql2012RetrospectionAccessor(final MsSql2012Dbms dbmsadaptor)
	{
		super(dbmsadaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardRetrospectionAccessor#getRetrospectionCodeGenerationNote()
	 */
	@Override
	public String getRetrospectionCodeGenerationNote()
	{
		return null;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_COLUMNS(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_COLUMNS(final SqlTableIdentity table)
	{
		/*
		 * SELECT COL.TABLE_SCHEMA, COL.TABLE_NAME, COL.COLUMN_NAME,
		 * COL.ORDINAL_POSITION, COL.COLUMN_DEFAULT, COL.IS_NULLABLE,
		 * COL.DATA_TYPE, COL.CHARACTER_MAXIMUM_LENGTH --,CASE WHEN
		 * TC.CONSTRAINT_TYPE IS NULL THEN 'NO' ELSE 'YES' END AS IS_UNIQUE
		 * 
		 * FROM INFORMATION_SCHEMA.COLUMNS COL
		 * 
		 * 
		 * --LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU ON
		 * CCU.COLUMN_NAME = COL.COLUMN_NAME -- AND CCU.TABLE_CATALOG =
		 * COL.TABLE_CATALOG -- AND CCU.TABLE_SCHEMA = COL.TABLE_SCHEMA -- AND
		 * CCU.TABLE_NAME = COL.TABLE_NAME -- --LEFT JOIN
		 * INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME =
		 * CCU.CONSTRAINT_NAME -- AND TC.TABLE_CATALOG = CCU.TABLE_CATALOG --
		 * AND TC.TABLE_SCHEMA = CCU.TABLE_SCHEMA -- AND TC.TABLE_NAME =
		 * CCU.TABLE_NAME -- AND TC.CONSTRAINT_CATALOG = CCU.CONSTRAINT_CATALOG
		 * -- AND TC.CONSTRAINT_SCHEMA = CCU.CONSTRAINT_SCHEMA
		 * 
		 * ORDER BY ORDINAL_POSITION ASC
		 */
		
		final String COL = getSystemTable_COLUMNS().sql().alias;
		// final String CCU = "CCU";
		// final String TCS = "TCS";
		
		final String COLd = COL + dot;
		// final String CCUd = CCU+dot;
		// final String TCSd = TCS+dot;
		
		String query = SELECT + NEW_LINE + __ + COLd
				+ Column_COLUMN_NAME
				+ comma
				+ NEW_LINE
				+ __
				+ COLd
				+ Column_DATA_TYPE
				+ comma
				+ NEW_LINE
				+ __
				+ COLd
				+ Column_IS_NULLABLE
				+ comma
				+ NEW_LINE
				+ __
				+ COLd
				+ Column_COLUMN_DEFAULT
				+ comma
				+ NEW_LINE
				+ __
				+ COLd
				+ Column_CHARACTER_MAXIMUM_LENGTH
				+ NEW_LINE
				+
				// comma+_+CASE+_+WHEN+_+TCSd+Column_CONSTRAINT_TYPE+_+IS_NULL+_+THEN+_+"'NO'"+_+ELSE+_+"'YES'"+_+END+_AS_+Column_IS_UNIQUE+NEW_LINE+
				
				FROM
				+ _
				+ getSystemTable_COLUMNS().util.toAliasString()
				+ NEW_LINE
				+
				
				// LEFT_JOIN+_+Table_CONSTRAINT_COLUMN_USAGE+_+CCU+_ON_+CCUd+Column_COLUMN_NAME+_eq_+COLd+Column_COLUMN_NAME+NEW_LINE+
				// __AND+_+CCUd+Column_TABLE_CATALOG+_eq_+COLd+Column_TABLE_CATALOG+NEW_LINE+
				// __AND+_+CCUd+Column_TABLE_SCHEMA+_eq_+COLd+Column_TABLE_SCHEMA+NEW_LINE+
				// __AND+_+CCUd+Column_TABLE_NAME+_eq_+COLd+Column_TABLE_NAME+NEW_LINE+
				//
				// LEFT_JOIN+_+Table_CONSTRAINT_TABLE_CONSTRAINTS+_+TCS+_ON_+TCSd+Column_CONSTRAINT_NAME+_eq_+CCUd+Column_CONSTRAINT_NAME+NEW_LINE+
				// __AND+_+TCSd+Column_TABLE_CATALOG+_eq_+CCUd+Column_TABLE_CATALOG+NEW_LINE+
				// __AND+_+TCSd+Column_TABLE_SCHEMA+_eq_+CCUd+Column_TABLE_SCHEMA+NEW_LINE+
				// __AND+_+TCSd+Column_TABLE_NAME+_eq_+CCUd+Column_TABLE_NAME+NEW_LINE+
				// __AND+_+TCSd+Column_CONSTRAINT_CATALOG+_eq_+CCUd+Column_CONSTRAINT_CATALOG+NEW_LINE+
				// __AND+_+TCSd+Column_CONSTRAINT_SCHEMA+_eq_+CCUd+Column_CONSTRAINT_SCHEMA+NEW_LINE+
				
				WHERE + _ + COLd + Column_TABLE_SCHEMA + _eq_ + apo + table.sql().schema + apo
				+ NEW_LINE + __AND + _ + COLd + Column_TABLE_NAME + _eq_ + apo + table.sql().name
				+ apo + NEW_LINE +
				
				ORDER_BY + _ + COLd + Column_ORDINAL_POSITION + _ + ASC;
		
		return query;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_INDICES(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_INDICES(final SqlTableIdentity table)
	{
		return SqlProc_EXEC_sp_helpindex + _ + apo + table + apo;
	}
	
	
	/**
	 * Creates the select_ informatio n_ schem a_ primarykey.
	 * 
	 * @param table
	 *            the table
	 * @return the string
	 */
	public String createSelect_INFORMATION_SCHEMA_PRIMARYKEY(final SqlTableIdentity table)
	{
		final String TCS = "TCS";
		final String TCSd = TCS + dot;
		
		/*
		 * SELECT T.CONSTRAINT_NAME, K.COLUMN_NAME
		 * 
		 * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
		 * 
		 * INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K ON K.CONSTRAINT_NAME
		 * = T.CONSTRAINT_NAME AND K.TABLE_CATALOG = T.TABLE_CATALOG AND
		 * K.TABLE_SCHEMA = T.TABLE_SCHEMA AND K.TABLE_NAME = T.TABLE_NAME
		 * 
		 * WHERE T.CONSTRAINT_TYPE = 'PRIMARY KEY' AND T.TABLE_SCHEMA = 'dbo'
		 * AND T.TABLE_NAME = 'MyTable'
		 * 
		 * ORDER BY K.ORDINAL_POSITION ASC
		 */
		
		final String query = SELECT + NEW_LINE + __ + TCSd + Column_CONSTRAINT_NAME + NEW_LINE +
		
		FROM + _ + Schema_INFORMATION_SCHEMA + dot + "TABLE_CONSTRAINTS" + _ + TCS + NEW_LINE +
		
		WHERE + _ + TCSd + Column_CONSTRAINT_TYPE + _eq_ + apo + "PRIMARY KEY" + apo + NEW_LINE
				+ __AND + _ + TCSd + Column_TABLE_SCHEMA + _eq_ + apo + table.sql().schema + apo
				+ NEW_LINE + __AND + _ + TCSd + Column_TABLE_NAME + _eq_ + apo + table.sql().name
				+ apo;
		
		return query;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#loadIndices(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public SqlIndex[] loadIndices(final SqlTableIdentity table) throws SQLEngineException
	{
		final String selectInformationSchemaIndices = createSelect_INFORMATION_SCHEMA_INDICES(table);
		final MsSql2012DDLMapper ddlMapper = this.getDbmsAdaptor().getDdlMapper();
		
		final DatabaseGateway<?> dbgw = getDbmsAdaptor().getDatabaseGateway();
		ResultSet rs;
		
		try
		{
			rs = dbgw.execute(SqlExecutor.query,selectInformationSchemaIndices);
		}
		catch(Exception e)
		{
			/*
			 * (01.03.2010 TM)NOTE: it can happen that EXEC sp_helpindex
			 * [tablename] returns no resultset which causes an exception. If
			 * this happens, print the error and return without indices.
			 */
			System.err.println("An error occured while querying index data for table "
					+ table.toString() + " via " + SqlProc_EXEC_sp_helpindex);
			System.err.println("Continuing without index generation");
			System.err.flush();
			e.printStackTrace();
			return new SqlIndex[0];
		}
		
		final ResultTable rt = new ResultTable(rs);
		final int rowCount = rt.getRowCount();
		
		final int colIdx_IndexName = rt.getColumnIndex(Column_index_name);
		final int colIdx_IndexDesc = rt.getColumnIndex(Column_index_description);
		final int colIdx_IndexCols = rt.getColumnIndex(Column_index_keys);
		
		final Object pkeyResult = getDbmsAdaptor().getDatabaseGateway().execute(
				SqlExecutor.singleResultQuery,createSelect_INFORMATION_SCHEMA_PRIMARYKEY(table));
		final String pkeyName = pkeyResult == null ? null : pkeyResult.toString();
		
		final SqlIndex[] indices = new SqlIndex[rowCount];
		INDEXTYPE type = null;
		String indexName = null;
		String[] columnList = null;
		for(int i = 0; i < rowCount; i++)
		{
			indexName = rt.getValue(i,colIdx_IndexName).toString();
			columnList = rt.getValue(i,colIdx_IndexCols).toString().split(", ");
			
			if(indexName.equals(pkeyName))
			{
				indices[i] = new SqlPrimaryKey(pkeyName,table,(Object[])columnList);
			}
			else
			{
				type = ddlMapper.mapIndexType(rt.getValue(i,colIdx_IndexDesc).toString());
				indices[i] = new SqlIndex(indexName,table,type,(Object[])columnList);
			}
		}
		
		return indices;
	}
	
	/*
	 * (10.02.2010 TM)NOTE: the following would be the code to determine if a
	 * column is UNIQUE (by itself) or not. Has been removed due to potential
	 * problems when defining a column as UNIQUE (in CREATE TABLE) and afterwars
	 * creating the single-column unique index explicitely. Instead, now the
	 * indirect UNIQUE column attribute is skipped and only the unique index is
	 * read. Makes everything a lot simpler and avoids the conflicts.
	 * 
	 * SELECT COL.TABLE_SCHEMA, COL.TABLE_NAME, COL.COLUMN_NAME,
	 * COL.ORDINAL_POSITION, COL.COLUMN_DEFAULT, COL.IS_NULLABLE, COL.DATA_TYPE,
	 * COL.CHARACTER_MAXIMUM_LENGTH, CASE WHEN ( SELECT COUNT(*) FROM
	 * INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU WHERE CCU.TABLE_CATALOG =
	 * COL.TABLE_CATALOG AND CCU.TABLE_SCHEMA = COL.TABLE_SCHEMA AND
	 * CCU.TABLE_NAME = COL.TABLE_NAME AND CCU.CONSTRAINT_NAME = ( SELECT
	 * CCUi.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCUi
	 * INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME
	 * = CCUi.CONSTRAINT_NAME AND TC.TABLE_CATALOG = COL.TABLE_CATALOG AND
	 * TC.TABLE_SCHEMA = COL.TABLE_SCHEMA AND TC.TABLE_NAME = COL.TABLE_NAME AND
	 * TC.CONSTRAINT_CATALOG = CCUi.CONSTRAINT_CATALOG AND TC.CONSTRAINT_SCHEMA
	 * = CCUi.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_TYPE = 'UNIQUE' WHERE
	 * CCUi.TABLE_CATALOG = COL.TABLE_CATALOG AND CCUi.TABLE_SCHEMA =
	 * COL.TABLE_SCHEMA AND CCUi.TABLE_NAME = COL.TABLE_NAME AND
	 * CCUi.COLUMN_NAME = COL.COLUMN_NAME ) ) = 1 THEN 'YES' ELSE 'NO' END AS
	 * IS_UNIQUE
	 * 
	 * FROM INFORMATION_SCHEMA.COLUMNS COL
	 * 
	 * LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU ON
	 * CCU.COLUMN_NAME = COL.COLUMN_NAME AND CCU.TABLE_CATALOG =
	 * COL.TABLE_CATALOG AND CCU.TABLE_SCHEMA = COL.TABLE_SCHEMA AND
	 * CCU.TABLE_NAME = COL.TABLE_NAME
	 * 
	 * LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME =
	 * CCU.CONSTRAINT_NAME AND TC.TABLE_CATALOG = CCU.TABLE_CATALOG AND
	 * TC.TABLE_SCHEMA = CCU.TABLE_SCHEMA AND TC.TABLE_NAME = CCU.TABLE_NAME AND
	 * TC.CONSTRAINT_CATALOG = CCU.CONSTRAINT_CATALOG AND TC.CONSTRAINT_SCHEMA =
	 * CCU.CONSTRAINT_SCHEMA
	 * 
	 * WHERE COL.TABLE_NAME = 'sysdiagrams'
	 * 
	 * ORDER BY ORDINAL_POSITION ASC
	 */
	
}
