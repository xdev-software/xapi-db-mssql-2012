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


import static com.xdev.jadoth.sqlengine.SQL.LANG.TOP;
import static com.xdev.jadoth.sqlengine.SQL.LANG.UNION;
import static com.xdev.jadoth.sqlengine.SQL.LANG.UNION_ALL;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.OMITALIAS;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.QUALIFY_BY_TABLE;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.UNQUALIFIED;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;

import com.xdev.jadoth.sqlengine.DELETE;
import com.xdev.jadoth.sqlengine.INSERT;
import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.UPDATE;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.internal.SqlExpression;
import com.xdev.jadoth.sqlengine.internal.SqlFunctionMOD;
import com.xdev.jadoth.sqlengine.types.ConditionalTableQuery;
import com.xdev.jadoth.sqlengine.types.WritingTableQuery;


/**
 * The Class MsSql2012DMLAssembler.
 */
public class MsSql2012DMLAssembler extends StandardDMLAssembler<MsSql2012Dbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	/** The Constant _TOP_. */
	protected static final String	_TOP_	= _ + TOP + _;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new ms sql2005 dml assembler.
	 * 
	 * @param dbms
	 *            the ms sql2005 dbms
	 */
	public MsSql2012DMLAssembler(final MsSql2012Dbms dbms)
	{
		super(dbms);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSELECT(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleSELECT(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query,sb,indentLevel,flags);
		this.assembleSelectRowLimit(query,sb,flags,clauseSeperator,newLine,indentLevel);
		this.assembleSelectItems(query,sb,flags,indentLevel,newLine);
		this.assembleSelectSqlClauses(query,sb,indentLevel,flags | ASEXPRESSION,clauseSeperator,
				newLine);
		this.assembleAppendSELECTs(query,sb,indentLevel,flags,clauseSeperator,newLine);
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleAppendSELECTs(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleAppendSELECTs(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		SELECT appendSelect = query.getUnionSelect();
		if(appendSelect != null)
		{
			this.assembleAppendSelect(appendSelect,sb,indentLevel,flags,clauseSeperator,newLine,
					UNION);
			return sb;
		}
		
		appendSelect = query.getUnionAllSelect();
		if(appendSelect != null)
		{
			this.assembleAppendSelect(appendSelect,sb,indentLevel,flags,clauseSeperator,newLine,
					UNION_ALL);
			return sb;
		}
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSelectRowLimit(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		final Integer top = query.getFetchFirstRowCount();
		if(top != null)
		{
			sb.append(_TOP_).append(top);
		}
		return sb;
	}
	
	
	/**
	 * Writing queries flags.
	 * 
	 * @param query
	 *            the query
	 * @param flags
	 *            the flags
	 * @return the int
	 */
	protected int writingQueriesFlags(final WritingTableQuery query, final int flags)
	{
		return flags
				| OMITALIAS
				| (query instanceof ConditionalTableQuery
						&& ((ConditionalTableQuery)query).getFromClause() != null ? QUALIFY_BY_TABLE
						: UNQUALIFIED);
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleDELETE(com.xdev.jadoth.sqlengine.DELETE,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleDELETE(final DELETE query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		return super.assembleDELETE(query,sb,this.writingQueriesFlags(query,flags),clauseSeperator,
				newLine,indentLevel);
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleINSERT(com.xdev.jadoth.sqlengine.INSERT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleINSERT(final INSERT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		return super.assembleINSERT(query,sb,this.writingQueriesFlags(query,flags),clauseSeperator,
				newLine,indentLevel);
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleUPDATE(com.xdev.jadoth.sqlengine.UPDATE,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleUPDATE(final UPDATE query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		return super.assembleUPDATE(query,sb,indentLevel,this.writingQueriesFlags(query,flags),
				clauseSeperator,newLine);
	}
	
	
	@Override
	public void assembleExpression(SqlExpression expression, StringBuilder sb, int indentLevel,
			int flags)
	{
		if(expression instanceof SqlFunctionMOD)
		{
			sb.append(((SqlFunctionMOD)expression).getParameters()[0] + " % "
					+ ((SqlFunctionMOD)expression).getParameters()[1]);
		}
		else
		{
			super.assembleExpression(expression,sb,indentLevel,flags);
		}
		
	}
	
}
