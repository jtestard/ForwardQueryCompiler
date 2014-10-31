/**
 * 
 */
package edu.ucsd.forward.query.function.pattern_matching;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.comparison.AbstractComparisonFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * @author Yupeng
 * 
 */
public class LikeFunction extends AbstractComparisonFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(LikeFunction.class);
    
    public static final String  NAME = "LIKE";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        LIKE_STRING;
    }
    
    /**
     * The default constructor.
     */
    public LikeFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "left";
        String right_name = "right";
        
        signature = new FunctionSignature(FunctionSignatureName.LIKE_STRING.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.STRING.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.INFIX;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value left_value = arg_values.get(0);
        Value right_value = arg_values.get(1);
        
        if (left_value instanceof NullValue || right_value instanceof NullValue)
        {
            return new BindingValue(new NullValue(BooleanType.class), true);
        }
        
        String string = ((StringValue) left_value).toString();
        String pattern = ((StringValue) right_value).toString();
        
        // Rewrite the pattern to regular expression
        pattern = rewriteToRegularExpression(pattern);
        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher m = p.matcher(string);
        
        // boolean result = m.matches();
        // GWT compliant
        boolean result = (m.find() && m.start() == 0 && m.end() == string.length()) ? true : false;
        
        return new BindingValue(new BooleanValue(result), true);
    }
    
    /**
     * Rewrites the pattern defined in SQL LIKE operator to regular expression.
     * 
     * @param pattern
     *            the pattern in SQL
     * @return the regular expression
     */
    private static String rewriteToRegularExpression(String pattern)
    {
        int len = pattern.length();
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++)
        {
            char c = pattern.charAt(i);
            if ("[](){}.*+?$^|#\\".indexOf(c) != -1)
            {
                sb.append("\\");
            }
            sb.append(c);
        }
        String re = sb.toString();
        
        // FIXME: Add the capability of escaping the character of % and _
        return re.replace("_", ".").replace("%", ".*");
        
    }
}
