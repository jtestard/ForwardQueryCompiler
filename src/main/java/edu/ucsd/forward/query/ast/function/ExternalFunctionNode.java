package edu.ucsd.forward.query.ast.function;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.logical.term.QueryPath;

/**
 * An external function call AST node, which has a name, a list of arguments, and a target data source. The arguments are child AST
 * nodes of the function call AST node.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ExternalFunctionNode extends GeneralFunctionNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FunctionNode.class);
    
    private String              m_target_data_source;
    
    /**
     * Constructs the function call with a name.
     * 
     * @param name
     *            the function call name.
     * @param target_data_source
     *            the target data source to evaluate the function.
     * @param location
     *            a location.
     */
    public ExternalFunctionNode(String name, String target_data_source, Location location)
    {
        super(name, location);
        
        assert (target_data_source != null && !target_data_source.isEmpty());
        m_target_data_source = target_data_source;
    }
    
    /**
     * Gets the target data source to evaluate the function.
     * 
     * @return the target data source to evaluate the function.
     */
    public String getTargetDataSource()
    {
        return m_target_data_source;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitExternalFunctionNode(this);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        // We assume the notation is PREFIX
        String qualified_name = m_target_data_source + QueryPath.PATH_SEPARATOR + this.getFunctionName();
        sb.append(this.getIndent(tabs) + qualified_name + QueryExpressionPrinter.LEFT_PAREN);
        
        for (ValueExpression argument : getArguments())
        {
            argument.toQueryString(sb, 0, data_source);
            sb.append(", ");
        }
        if (!getArguments().isEmpty()) sb.delete(sb.length() - 2, sb.length());
        sb.append(QueryExpressionPrinter.RIGHT_PAREN);
        
    }
    
    @Override
    public AstNode copy()
    {
        ExternalFunctionNode copy = new ExternalFunctionNode(this.getFunctionName(), m_target_data_source, this.getLocation());
        
        super.copy(copy);
        
        for (ValueExpression expr : getArguments())
        {
            copy.addArgument((ValueExpression) expr.copy());
        }
        
        return copy;
    }
    
}
