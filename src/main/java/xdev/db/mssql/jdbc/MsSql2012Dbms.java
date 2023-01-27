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

import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


/**
 * The Class MsSql2012Dbms.
 */
public class MsSql2012Dbms
		extends
		DbmsAdaptor.Implementation<MsSql2012Dbms, MsSql2012DMLAssembler, MsSql2012DDLMapper, MsSql2012RetrospectionAccessor, MsSql2012Syntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int	MAX_VARCHAR_LENGTH		= 8000;
	
	protected static final char	IDENTIFIER_DELIMITER	= '"';
	
	
	// /////////////////////////////////////////////////////////////////////////
	// static methods //
	// /////////////////
	
	/**
	 * Single connection.
	 * 
	 * @param host
	 *            the host
	 * @param port
	 *            the port
	 * @param user
	 *            the user
	 * @param password
	 *            the password
	 * @param database
	 *            the database
	 * @param properties
	 *            the extended url properties
	 * @return the connection provider
	 */
	public static ConnectionProvider<MsSql2012Dbms> singleConnection(final String host,
			final int port, final String user, final String password, final String database,
			final String properties)
	{
		return new ConnectionProvider.Body<>(new MsSql2012ConnectionInformation(host,
				port,user,password,database,properties,new MsSql2012Dbms()));
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////
	
	/**
	 * Instantiates a new ms sql2012 dbms.
	 */
	public MsSql2012Dbms()
	{
		this(new MsSql2012ExceptionParser());
	}
	
	
	/**
	 * Instantiates a new ms sql2012 dbms.
	 * 
	 * @param sqlExceptionParser
	 *            the sql exception parser
	 */
	public MsSql2012Dbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new MsSql2012RetrospectionAccessor(this));
		this.setDMLAssembler(new MsSql2012DMLAssembler(this));
		this.setDdlMapper(new MsSql2012DDLMapper(this));
		this.setSyntax(new MsSql2012Syntax());
	}
	
	
	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param catalog
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#createConnectionInformation(java.lang.String,
	 *      int, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public MsSql2012ConnectionInformation createConnectionInformation(final String host,
			final int port, final String user, final String password, final String catalog,
			final String properties)
	{
		return new MsSql2012ConnectionInformation(host,port,user,password,catalog,properties,this);
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#updateSelectivity(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	
	/**
	 * @param bytes
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return sb;
	}
	
	
	/**
	 * @param dbc
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<MsSql2012Dbms> dbc)
	{
		// No initialization needed
	}
	
	
	/**
	 * @param fullQualifiedTableName
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return false;
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
	
}
