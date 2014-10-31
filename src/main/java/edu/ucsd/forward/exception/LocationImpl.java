/**
 * 
 */
package edu.ucsd.forward.exception;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.parser.Token;

/**
 * Implementation if the location interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class LocationImpl implements Location
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(LocationImpl.class);
    
    private int                 m_start_line;
    
    private int                 m_start_column;
    
    private int                 m_end_line;
    
    private int                 m_end_column;
    
    private String              m_path;
    
    /**
     * Copy constructor.
     * 
     * @param location
     *            the location to copy.
     */
    public LocationImpl(Location location)
    {
        // FIXME Change this check into an assertion that makes sure that the location is never null
        if (location == null)
        {
            this.setPath(Location.UNKNOWN_PATH);
        }
        else
        {
            this.setPath(location.getPath());
            
            if (location.getStartLine() > 0) this.setStartLine(location.getStartLine());
            if (location.getStartColumn() > 0) this.setStartColumn(location.getStartColumn());
            if (location.getEndLine() > 0) this.setEndLine(location.getEndLine());
            if (location.getEndColumn() > 0) this.setEndColumn(location.getEndColumn());
        }
    }
    
    /**
     * Constructor.
     */
    public LocationImpl()
    {
        this(Location.UNKNOWN_PATH);
    }
    
    /**
     * Constructor.
     * 
     * @param path
     *            Path of resource. This path should be retrieved from URL.toString() and use "/" as the separator.
     */
    public LocationImpl(String path)
    {
        this.setPath(path);
    }
    
    /**
     * Constructor.
     * 
     * @param line
     *            the line of the location.
     * @param column
     *            the column of the location.
     */
    public LocationImpl(int line, int column)
    {
        this(Location.UNKNOWN_PATH, line, column);
    }
    
    /**
     * Constructor.
     * 
     * @param path
     *            Path of resource. This path should be retrieved from URL.toString() and use "/" as the separator.
     * @param line
     *            the line of the location.
     * @param column
     *            the column of the location.
     */
    public LocationImpl(String path, int line, int column)
    {
        this.setPath(path);
        this.setStartLine(line);
        this.setStartColumn(column);
    }
    
    /**
     * Constructor.
     * 
     * @param start_line
     *            the starting line of the location.
     * @param start_column
     *            the starting column of the location.
     * @param end_line
     *            the ending line of the location.
     * @param end_column
     *            the ending column of the location.
     * 
     */
    public LocationImpl(int start_line, int start_column, int end_line, int end_column)
    {
        this(Location.UNKNOWN_PATH, start_line, start_column, end_line, end_column);
    }
    
    /**
     * Constructor.
     * 
     * @param path
     *            Path of resource. This path should be retrieved from URL.toString() and use "/" as the separator.
     * 
     * @param start_line
     *            the starting line of the location.
     * @param start_column
     *            the starting column of the location.
     * @param end_line
     *            the ending line of the location.
     * @param end_column
     *            the ending column of the location.
     * 
     */
    public LocationImpl(String path, int start_line, int start_column, int end_line, int end_column)
    {
        this.setPath(path);
        this.setStartLine(start_line);
        this.setStartColumn(start_column);
        this.setEndLine(end_line);
        this.setEndColumn(end_column);
    }
    
    /**
     * Constructor based on a query parser token that determines the start line and column.
     * 
     * @param start
     *            a query parser token.
     * @param path
     *            path of the source file the query is located
     * @param offset_line
     *            which line the start of query is located in the source file
     * @param offset_column
     *            which column the start of the query is located in the source file
     */
    public LocationImpl(Token start, String path, int offset_line, int offset_column)
    {
        this(path == null ? Location.UNKNOWN_PATH : path, start.beginLine + offset_line, start.beginColumn
                + (start.beginLine == 1 ? offset_column : 0));
    }
    
    /**
     * Constructor based on query parser tokens that determine the start line and column and end line and column.
     * 
     * @param start
     *            a query parser token.
     * @param end
     *            a query parser token.
     * @param path
     *            path of the source file the query is located
     * @param offset_line
     *            which line the start of query is located in the source file
     * @param offset_column
     *            which column the start of the query is located in the source file
     */
    public LocationImpl(Token start, Token end, String path, int offset_line, int offset_column)
    {
        this(path == null ? Location.UNKNOWN_PATH : path, start.beginLine + offset_line, start.beginColumn
                + (start.beginLine == 1 ? offset_column : 0), end.endLine + offset_line, end.endColumn
                + (end.endLine == 1 ? offset_column : 0));
    }
    
    @Override
    public String getPath()
    {
        return m_path;
    }
    
    @Override
    public void setPath(String path)
    {
        if (path == null)
        {
            m_path = LocationImpl.UNKNOWN_PATH;
        }
        else
        {
            m_path = path;
        }
    }
    
    @Override
    public int getStartLine()
    {
        return m_start_line;
    }
    
    @Override
    public void setStartLine(int start_line)
    {
        m_start_line = start_line;
    }
    
    @Override
    public int getStartColumn()
    {
        return m_start_column;
    }
    
    @Override
    public void setStartColumn(int start_column)
    {
        m_start_column = start_column;
    }
    
    @Override
    public int getEndLine()
    {
        return m_end_line;
    }
    
    @Override
    public void setEndLine(int end_line)
    {
        m_end_line = end_line;
    }
    
    @Override
    public int getEndColumn()
    {
        return m_end_column;
    }
    
    @Override
    public void setEndColumn(int end_column)
    {
        m_end_column = end_column;
    }
    
    @Override
    public String toString()
    {
        String out = "";
        
        if (m_path != null && m_path.length() > 0 && !m_path.equals(UNKNOWN_PATH))
        {
            out += m_path + " ";
        }
        
        if (m_start_line > 0)
        {
            out += "(" + m_start_line;
            if (m_start_column > 0)
            {
                out += ":" + m_start_column;
            }
            if (m_end_line > 0)
            {
                out += "-" + m_end_line;
                if (m_end_column > 0)
                {
                    out += ":" + m_end_column;
                }
            }
            out += ")";
        }
        
        return out;
    }
}
