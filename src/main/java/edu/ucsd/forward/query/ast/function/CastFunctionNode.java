package edu.ucsd.forward.query.ast.function;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.jdbc.model.SqlTypeConverter;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.function.cast.CastFunction;

/**
 * A cast function call, which has a fixed name, a single attribute, and a target type.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class CastFunctionNode extends AbstractFunctionNode
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CastFunctionNode.class);
    
    private static final String NAME = "CAST";
    
    /**
     * The target type name.
     */
    private String              m_target_type_name;
    
    /**
     * Constructs the function call with an argument.
     * 
     * @param argument
     *            the argument.
     * @param target_type_name
     *            the target type name.
     * @param location
     *            a location.
     */
    public CastFunctionNode(ValueExpression argument, String target_type_name, Location location)
    {
        super(CastFunction.NAME, location);
        
        assert (argument != null);
        this.addChild(argument);
        
        assert (target_type_name != null);
        m_target_type_name = target_type_name;
    }
    
    /**
     * Gets the function call argument.
     * 
     * @return the function call argument.
     */
    public ValueExpression getArgument()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.size() == 1);
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    /**
     * Sets the argument.
     * 
     * @param argument
     *            the argument to set.
     */
    public void setArgument(ValueExpression argument)
    {
        if (getArgument() != null)
        {
            this.removeChild(getArgument());
        }
        
        assert (argument != null);
        this.addChild(argument);
    }
    
    /**
     * Gets the function call target type name.
     * 
     * @return the function call target type name.
     */
    public String getTargetTypeName()
    {
        return m_target_type_name;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + NAME + QueryExpressionPrinter.LEFT_PAREN);
        getArgument().toQueryString(sb, 0, data_source);
        
        DataSourceMetaData metadata = null;
        try
        {
            metadata = (data_source != null)
                    ? data_source.getMetaData()
                    : QueryProcessorFactory.getInstance().getDataSourceMetaData(DataSource.MEDIATOR);
        }
        catch (DataSourceException e)
        {
            // This will never happen
            assert (false);
        }
        
        String type_name = m_target_type_name;
        switch (metadata.getStorageSystem())
        {
            case INMEMORY:
                break;
            case JDBC:
                try
                {
                    Type target_type = TypeEnum.get(type_name);
                    assert (target_type instanceof ScalarType);
                    type_name = SqlTypeConverter.getSqlTypeName(((ScalarType) target_type).getClass());
                }
                catch (TypeException e)
                {
                    // FIXME Throw unknown type exception
                    assert (false);
                }
                break;
            default:
                throw new AssertionError();
        }
        
        sb.append(" AS " + type_name + QueryExpressionPrinter.RIGHT_PAREN);
        
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        out += " -> " + NAME + " AS " + m_target_type_name;
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitCastFunctionNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        CastFunctionNode copy = new CastFunctionNode((ValueExpression) getArgument().copy(), m_target_type_name, this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
