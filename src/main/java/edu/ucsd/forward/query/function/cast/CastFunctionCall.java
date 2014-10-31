package edu.ucsd.forward.query.function.cast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.UnknownType;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * A cast function call, which has a cast function definition and a list of arguments. The arguments are terms, which can be other
 * cast function calls, query paths or constants.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class CastFunctionCall extends AbstractFunctionCall<CastFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CastFunctionCall.class);
    
    /**
     * The target type.
     */
    private TypeEnum            m_target_type;
    
    /**
     * Constructs the cast function call for a given argument.
     * 
     * @param argument
     *            the function argument.
     * @param target_type
     *            the target type.
     */
    public CastFunctionCall(Term argument, TypeEnum target_type)
    {
        super();
        
        try
        {
            this.setFunction((CastFunction) FunctionRegistry.getInstance().getFunction(CastFunction.NAME));
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen
            assert (false);
        }
        
        assert (argument != null);
        this.addArgument(argument);
        
        assert (target_type != null);
        m_target_type = target_type;
        
        this.setType(m_target_type.get());
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private CastFunctionCall()
    {
        super();
    }
    
    /**
     * Gets the function call target type.
     * 
     * @return the function call target type.
     */
    public TypeEnum getTargetType()
    {
        return m_target_type;
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        // Get the output types of the arguments
        Type argument_type = this.getArguments().get(0).inferType(operators);
        
        // Infeasible conversion
        if (!(argument_type instanceof JsonType) && !(argument_type instanceof UnknownType)
                && !TypeConverter.getInstance().canExplicitlyConvert(argument_type, m_target_type.get()))
        {
            throw new QueryCompilationException(QueryCompilation.INVALID_TYPE_CONVERION, this.getLocation(),
                                                TypeEnum.getName(argument_type), m_target_type.getName());
        }
        
        // Complex target types cannot be directly returned since they are only names (TypeEnum entries) and do not have structure.
        // For that reason, we have to use the type of the argument.
        if (m_target_type == TypeEnum.COLLECTION)
        {
            assert (argument_type instanceof CollectionType);
        }
        else if (m_target_type == TypeEnum.TUPLE)
        {
            if (argument_type instanceof TupleType)
            {
                assert (true);
            }
            else
            {
                assert (argument_type instanceof CollectionType);
                
                argument_type = ((CollectionType) argument_type).getTupleType();
            }
        }
        else
        {
            argument_type = m_target_type.get();
        }
        
        this.setType(argument_type);
        
        return this.getType();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // No need to check compliant data sources, since it's an empty list.
        
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                // Always compliant with SQL++ data sources
                return true;
            case RELATIONAL:
                // Not always compliant with relational data sources
                if (!(m_target_type.get() instanceof ScalarType))
                {
                    return false;
                }
                
                return super.isDataSourceCompliant(metadata);
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String toExplainString()
    {
        String str = this.getFunction().getName() + "(";
        str += this.getArguments().get(0).toExplainString() + " AS " + m_target_type.getName();
        str += ")";
        
        return str;
    }
    
    @Override
    public Term copy()
    {
        CastFunctionCall copy = new CastFunctionCall(this.getArguments().get(0).copy(), m_target_type);
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Term copyWithoutType()
    {
        CastFunctionCall copy = new CastFunctionCall(this.getArguments().get(0).copyWithoutType(), m_target_type);
        
        return copy;
    }
}
