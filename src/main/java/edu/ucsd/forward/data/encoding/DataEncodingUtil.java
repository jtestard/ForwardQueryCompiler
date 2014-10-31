/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.FloatType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.NumericType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TimestampType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;

/**
 * @author AdityaAvinash
 * 
 *         Utility methods for encoding and decoding data and schema
 */
public class DataEncodingUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataEncodingUtil.class);
    
    /**
     * @param type
     * @return Code
     */
    public static TypeCode findType(Type type)
    {
        if (type == null) return null;
        if ((type.getClass()).equals(CollectionType.class))
        {
            if (((CollectionType) type).isOrdered())
            {
                return TypeCode.ARRAY_TYPE;
            }
            else
            {
                return TypeCode.BAG_TYPE;
            }
        }
        else if ((type.getClass()).equals(TupleType.class))
        {
            return TypeCode.TUPLE_TYPE;
        }
        else if ((type.getClass()).equals(BooleanType.class))
        {
            return TypeCode.BOOLEAN_TYPE;
        }
        else if ((type.getClass()).equals(IntegerType.class))
        {
            return TypeCode.INTEGER_TYPE;
        }
        else if ((type.getClass()).equals(LongType.class))
        {
            return TypeCode.LONG_TYPE;
        }
        else if ((type.getClass()).equals(FloatType.class))
        {
            return TypeCode.FLOAT_TYPE;
        }
        else if ((type.getClass()).equals(DoubleType.class))
        {
            return TypeCode.DOUBLE_TYPE;
        }
        else if ((type.getClass()).equals(StringType.class))
        {
            return TypeCode.STRING_TYPE;
        }
        else if ((type.getClass()).equals(TimestampType.class))
        {
            return TypeCode.ENRICHED_TYPE;
            /*
             * }else if((type.getClass()).equals(FAR_POINTER_TYPE.class)){ return Code.FAR_POINTER_TYPE; }else
             * if((type.getClass()).equals(NEAR_POINTER_TYPE.class)){ return Code.NEAR_POINTER_TYPE; }else
             * if((type.getClass()).equals(UNION_TYPE.class)){ return Code.UNION_TYPE; }else
             * if((type.getClass()).equals(ANY_TYPE.class)){ return Code.ANY_TYPE;
             */
        }
        else
        {
            return null;
        }
        
    }
}
