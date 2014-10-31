/**
 * 
 */
package edu.ucsd.forward.query.explain;

import edu.ucsd.forward.data.source.DataSource;

/**
 * Generates a SQL++ query string.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface QueryExpressionPrinter
{
    public static final String TAB         = "   ";
    
    public static final String NL          = "\n";
    
    public static final String LEFT_PAREN  = "(";
    
    public static final String RIGHT_PAREN = ")";
    
    /**
     * Returns the query string that can executed by the input data source. The output query string cannot always be parsed back
     * into an AST by the query parser, since the dialect of the target data source might be different than the SQL++ dialect.
     * 
     * @param builder
     *            the string builder to append characters to.
     * @param tabs
     *            number of leading tabs.
     * @param data_source
     *            the data source that will execute the query string. If null, then the DataSource.MEDIATOR is assumed.
     * 
     * @return a SQL++ query string.
     */
    public void toQueryString(StringBuilder builder, int tabs, DataSource data_source);
    
}
