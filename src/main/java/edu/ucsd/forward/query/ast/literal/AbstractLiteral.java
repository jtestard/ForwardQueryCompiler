/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.ast.AbstractValueExpression;

/**
 * Represents an abstract implementation of the scalar literal interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractLiteral extends AbstractValueExpression implements Literal
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ScalarLiteral.class);
    
    private String              m_literal;
    
    /**
     * Creates an instance of the literal.
     * 
     * @param literal
     *            the literal.
     * @param location
     *            a location.
     */
    public AbstractLiteral(String literal, Location location)
    {
        super(location);
        
        assert (literal != null);
        m_literal = literal;
    }
    
    @Override
    public String getLiteral()
    {
        return m_literal;
    }
    
    @Override
    public void setLiteral(String literal)
    {
        m_literal = literal;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + m_literal);
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        out += " -> " + m_literal;
        
        return out;
    }
    
}
