package edu.ucsd.forward.query.function.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * A tuple function call, which has a tuple function definition and a list of arguments. The arguments are terms, which can be other
 * function calls, query paths or constants.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng Fu
 * 
 */
@SuppressWarnings("serial")
public class TupleFunctionCall extends AbstractFunctionCall<TupleFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log      = Logger.getLogger(TupleFunctionCall.class);
    
    private boolean             m_inline = false;
    
    /**
     * Constructs the tuple function call for a given list of arguments. The constructor copies the arguments into an internal list.
     * 
     * @param arguments
     *            the function arguments.
     */
    public TupleFunctionCall(List<Term> arguments)
    {
        super();
        
        assert (arguments != null);
        
        try
        {
            this.setFunction((TupleFunction) FunctionRegistry.getInstance().getFunction(TupleFunction.NAME));
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen
            assert (false);
        }
        
        for (Term arg : arguments)
            this.addArgument(arg);
    }
    
    /**
     * Checks if the tuple call is set as inline.
     * 
     * @return whether the tuple call is set as inline.
     */
    public boolean isInline()
    {
        return m_inline;
    }
    
    /**
     * Sets the tuple call as inline.
     * 
     * @param inline
     *            inline value.
     */
    public void setInline(boolean inline)
    {
        m_inline = inline;
    }
    
    /**
     * Constructs the tuple function call for a given variable number of arguments. The constructor copies the arguments into an
     * internal list. This constructor is mainly used in test cases.
     * 
     * @param arguments
     *            the function arguments.
     */
    public TupleFunctionCall(Term... arguments)
    {
        this(Arrays.asList(arguments));
    }
    
    public TupleFunctionCall()
    {
        this(new ArrayList<Term>());
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        TupleType tuple_type = new TupleType();
        
        tuple_type.setInline(isInline());
        
        // Make sure the number of arguments is even
        assert (this.getArguments().size() % 2 == 0);
        
        Iterator<Term> iter_args = this.getArguments().iterator();
        while (iter_args.hasNext())
        {
            Term arg = iter_args.next();
            Type attr_type = arg.inferType(operators);
            attr_type = TypeUtil.cloneNoParent(attr_type);
            
            Term attr_name_term = iter_args.next();
            assert (attr_name_term instanceof Constant);
            Constant attr_name_const = (Constant) attr_name_term;
            attr_name_const.inferType(operators);
            assert (attr_name_const.getType().getClass() == StringType.class);
            assert (attr_name_const.getValue() != null);
            String attr_name = attr_name_const.getValue().toString();
            
            tuple_type.setAttributeNoCheck(attr_name, attr_type);
        }
        
        this.setType(tuple_type);
        
        return this.getType();
    }
    
    @Override
    public String toExplainString()
    {
        String str = "";
        if (isInline()) str += "INLINE ";
        
        str += this.getFunction().getName() + "(";
        
        Iterator<Term> iter = this.getArguments().iterator();
        while(iter.hasNext())
        {
            str += iter.next().toExplainString() + " AS ";
            str += iter.next().toExplainString() +", ";
        }
            
        if (!this.getArguments().isEmpty()) str = str.substring(0, str.length() - 2);
        str += ")";
        
        return str;
    }
    
    @Override
    public Term copy()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copy());
        }
        TupleFunctionCall copy = new TupleFunctionCall(copies);
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Term copyWithoutType()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copyWithoutType());
        }
        TupleFunctionCall copy = new TupleFunctionCall(copies);
        
        return copy;
    }
}
