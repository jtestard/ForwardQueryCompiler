/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.collection.Pair;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DateValue;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * The type converter for the data model.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class TypeConverter
{
    private static final TypeConverter                INSTANCE          = new TypeConverter();
    
    private Map<Pair<String, String>, TypeConversion> m_conversion_map;
    
    private static final BigDecimal                   MIN_VALUE_INTEGER = new BigDecimal(Integer.MIN_VALUE);
    
    private static final BigDecimal                   MAX_VALUE_INTEGER = new BigDecimal(Integer.MAX_VALUE);
    
    private static final BigDecimal                   MIN_VALUE_LONG    = new BigDecimal(Long.MIN_VALUE);
    
    private static final BigDecimal                   MAX_VALUE_LONG    = new BigDecimal(Long.MAX_VALUE);
    
    private static final BigDecimal                   MIN_VALUE_FLOAT   = new BigDecimal(-Float.MAX_VALUE);
    
    private static final BigDecimal                   MAX_VALUE_FLOAT   = new BigDecimal(Float.MAX_VALUE);
    
    private static final BigDecimal                   MIN_VALUE_DOUBLE  = new BigDecimal(-Double.MAX_VALUE);
    
    private static final BigDecimal                   MAX_VALUE_DOUBLE  = new BigDecimal(Double.MAX_VALUE);
    
    /**
     * A secondary type conversion checker especially needed for complex types.
     * 
     * @author Michalis Petropoulos
     */
    private interface Checker
    {
        /**
         * Checks if the source type can be converted to the target type.
         * 
         * @param source
         *            the source type.
         * @param implicit
         *            whether the check is for an implicit conversion.
         * @return true, if the source type can be converted to the target type, otherwise, false.
         */
        boolean check(Type source, boolean implicit);
    }
    
    /**
     * Performs a type conversion.
     * 
     * @author Michalis Petropoulos
     */
    private interface Performer
    {
        /**
         * Converts the input value.
         * 
         * @param source_value
         *            a source value.
         * @return the converted value.
         * @throws TypeException
         *             if there is a type exception during conversion.
         */
        Value convert(Value source_value) throws TypeException;
    }
    
    /**
     * The list of explicit type conversions supported by the data model. Some of the explicit type conversions can also be
     * implicit, which is captured by the implicit member variable.
     * 
     * @author Michalis Petropoulos
     * 
     */
    private enum TypeConversion
    {
        COLLECTION_TO_COLLECTION(CollectionType.class, CollectionType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        COLLECTION_TO_TUPLE(CollectionType.class, TupleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                CollectionValue coll_value = (CollectionValue) source_value;
                
                if (coll_value.size() == 1) return coll_value.getTuples().get(0);
                
                if (coll_value.size() == 0) return new NullValue(TupleType.class);
                
                throw new TypeException(ExceptionMessages.Type.INVALID_COLLECTION_TO_TUPLE_CONVERION);
            }
        }),

        COLLECTION_TO_DECIMAL(CollectionType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.DECIMAL.get(),
                                                                           implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.DECIMAL.get());
            }
        }),

        COLLECTION_TO_DOUBLE(CollectionType.class, DoubleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.DOUBLE.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.DOUBLE.get());
            }
        }),

        COLLECTION_TO_FLOAT(CollectionType.class, FloatType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.FLOAT.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.FLOAT.get());
            }
        }),

        COLLECTION_TO_LONG(CollectionType.class, LongType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.LONG.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.LONG.get());
            }
        }),

        COLLECTION_TO_INTEGER(CollectionType.class, IntegerType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.INTEGER.get(),
                                                                           implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.INTEGER.get());
            }
        }),

        COLLECTION_TO_DATE(CollectionType.class, DateType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.DATE.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.DATE.get());
            }
        }),

        COLLECTION_TO_TIMESTAMP(CollectionType.class, TimestampType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.TIMESTAMP.get(),
                                                                           implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.TIMESTAMP.get());
            }
        }),

        COLLECTION_TO_BOOLEAN(CollectionType.class, BooleanType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.BOOLEAN.get(),
                                                                           implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.BOOLEAN.get());
            }
        }),

        COLLECTION_TO_XHTML(CollectionType.class, XhtmlType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.XHTML.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.XHTML.get());
            }
        }),

        COLLECTION_TO_STRING(CollectionType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkCollectionToScalar((CollectionType) source, TypeEnum.STRING.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertCollectionToScalar(source_value, TypeEnum.STRING.get());
            }
        }),

        TUPLE_TO_TUPLE(TupleType.class, TupleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        TUPLE_TO_DECIMAL(TupleType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.DECIMAL.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.DECIMAL.get());
            }
        }),

        TUPLE_TO_DOUBLE(TupleType.class, DoubleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.DOUBLE.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.DOUBLE.get());
            }
        }),

        TUPLE_TO_FLOAT(TupleType.class, FloatType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.FLOAT.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.FLOAT.get());
            }
        }),

        TUPLE_TO_LONG(TupleType.class, LongType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.LONG.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.LONG.get());
            }
        }),

        TUPLE_TO_INTEGER(TupleType.class, IntegerType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.INTEGER.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.INTEGER.get());
            }
        }),

        TUPLE_TO_DATE(TupleType.class, DateType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.DATE.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.DATE.get());
            }
        }),

        TUPLE_TO_TIMESTAMP(TupleType.class, TimestampType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.TIMESTAMP.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.TIMESTAMP.get());
            }
        }),

        TUPLE_TO_BOOLEAN(TupleType.class, BooleanType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.BOOLEAN.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.BOOLEAN.get());
            }
        }),

        TUPLE_TO_XHTML(TupleType.class, XhtmlType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.XHTML.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.XHTML.get());
            }
        }),

        TUPLE_TO_STRING(TupleType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return TypeConverter.getInstance().checkTupleToScalar((TupleType) source, TypeEnum.STRING.get(), implicit);
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                return TypeConverter.getInstance().convertTupleToScalar(source_value, TypeEnum.STRING.get());
            }
        }),

        SWITCH_TO_SWITCH(SwitchType.class, SwitchType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        INTEGER_TO_DECIMAL(IntegerType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DecimalValue(new BigDecimal(((IntegerValue) source_value).getObject().intValue()));
            }
        }),

        INTEGER_TO_DOUBLE(IntegerType.class, DoubleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DoubleValue(((IntegerValue) source_value).getObject().doubleValue());
            }
        }),

        INTEGER_TO_FLOAT(IntegerType.class, FloatType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new FloatValue(((IntegerValue) source_value).getObject().floatValue());
            }
        }),

        INTEGER_TO_LONG(IntegerType.class, LongType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new LongValue(((IntegerValue) source_value).getObject().longValue());
            }
        }),

        INTEGER_TO_INTEGER(IntegerType.class, IntegerType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        INTEGER_TO_STRING(IntegerType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((IntegerValue) source_value).getObject().toString());
            }
        }),

        LONG_TO_DECIMAL(LongType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DecimalValue(new BigDecimal(((LongValue) source_value).getObject().longValue()));
            }
        }),

        LONG_TO_DOUBLE(LongType.class, DoubleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DoubleValue(((LongValue) source_value).getObject().doubleValue());
            }
        }),

        LONG_TO_FLOAT(LongType.class, FloatType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new FloatValue(((LongValue) source_value).getObject().floatValue());
            }
        }),

        LONG_TO_LONG(LongType.class, LongType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        LONG_TO_INTEGER(LongType.class, IntegerType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((LongValue) source_value).getObject().compareTo(Long.valueOf(Integer.MIN_VALUE)) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                if (((LongValue) source_value).getObject().compareTo(Long.valueOf(Integer.MAX_VALUE)) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                
                return new IntegerValue(((LongValue) source_value).getObject().intValue());
            }
        }),

        LONG_TO_STRING(LongType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((LongValue) source_value).getObject().toString());
            }
        }),

        FLOAT_TO_DECIMAL(FloatType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DecimalValue(new BigDecimal(((FloatValue) source_value).getObject().doubleValue()));
            }
        }),

        FLOAT_TO_DOUBLE(FloatType.class, DoubleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DoubleValue(((FloatValue) source_value).getObject().doubleValue());
            }
        }),

        FLOAT_TO_FLOAT(FloatType.class, FloatType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        FLOAT_TO_LONG(FloatType.class, LongType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((FloatValue) source_value).getObject().compareTo(Float.valueOf(Long.MIN_VALUE)) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.LONG.getName());
                }
                if (((FloatValue) source_value).getObject().compareTo(Float.valueOf(Long.MAX_VALUE)) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.LONG.getName());
                }
                
                return new LongValue(((FloatValue) source_value).getObject().longValue());
            }
        }),

        FLOAT_TO_INTEGER(FloatType.class, IntegerType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((FloatValue) source_value).getObject().compareTo(Float.valueOf(Integer.MIN_VALUE)) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                if (((FloatValue) source_value).getObject().compareTo(Float.valueOf(Integer.MAX_VALUE)) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                
                return new IntegerValue(((FloatValue) source_value).getObject().intValue());
            }
        }),

        FLOAT_TO_STRING(FloatType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((FloatValue) source_value).getObject().toString());
            }
        }),

        DOUBLE_TO_DECIMAL(DoubleType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new DecimalValue(new BigDecimal(((DoubleValue) source_value).getObject().doubleValue()));
            }
        }),

        DOUBLE_TO_DOUBLE(DoubleType.class, DoubleType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        DOUBLE_TO_FLOAT(DoubleType.class, FloatType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DoubleValue) source_value).getObject().compareTo(Double.valueOf(-Float.MAX_VALUE)) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.FLOAT.getName());
                }
                if (((DoubleValue) source_value).getObject().compareTo(Double.valueOf(Float.MAX_VALUE)) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.FLOAT.getName());
                }
                
                return new FloatValue(((DoubleValue) source_value).getObject().floatValue());
            }
        }),

        DOUBLE_TO_LONG(DoubleType.class, LongType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DoubleValue) source_value).getObject().compareTo(Double.valueOf(Long.MIN_VALUE)) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.LONG.getName());
                }
                if (((DoubleValue) source_value).getObject().compareTo(Double.valueOf(Long.MAX_VALUE)) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.LONG.getName());
                }
                
                return new LongValue(((DoubleValue) source_value).getObject().longValue());
            }
        }),

        DOUBLE_TO_INTEGER(DoubleType.class, IntegerType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DoubleValue) source_value).getObject().compareTo(Double.valueOf(Integer.MIN_VALUE)) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                if (((DoubleValue) source_value).getObject().compareTo(Double.valueOf(Integer.MAX_VALUE)) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                
                return new IntegerValue(((DoubleValue) source_value).getObject().intValue());
            }
        }),

        DOUBLE_TO_STRING(DoubleType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((DoubleValue) source_value).getObject().toString());
            }
        }),

        DECIMAL_TO_DECIMAL(DecimalType.class, DecimalType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        DECIMAL_TO_DOUBLE(DecimalType.class, DoubleType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DecimalValue) source_value).getObject().compareTo(MIN_VALUE_DOUBLE) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.DOUBLE.getName());
                }
                if (((DecimalValue) source_value).getObject().compareTo(MAX_VALUE_DOUBLE) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.DOUBLE.getName());
                }
                
                return new DoubleValue(((DecimalValue) source_value).getObject().doubleValue());
            }
        }),

        DECIMAL_TO_FLOAT(DecimalType.class, FloatType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DecimalValue) source_value).getObject().compareTo(MIN_VALUE_FLOAT) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.FLOAT.getName());
                }
                if (((DecimalValue) source_value).getObject().compareTo(MAX_VALUE_FLOAT) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.FLOAT.getName());
                }
                
                return new FloatValue(((DecimalValue) source_value).getObject().floatValue());
            }
        }),

        DECIMAL_TO_LONG(DecimalType.class, LongType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DecimalValue) source_value).getObject().compareTo(MIN_VALUE_LONG) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.LONG.getName());
                }
                if (((DecimalValue) source_value).getObject().compareTo(MAX_VALUE_LONG) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.LONG.getName());
                }
                
                return new LongValue(((DecimalValue) source_value).getObject().longValue());
            }
        }),

        DECIMAL_TO_INTEGER(DecimalType.class, IntegerType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                if (((DecimalValue) source_value).getObject().compareTo(MIN_VALUE_INTEGER) < 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                if (((DecimalValue) source_value).getObject().compareTo(MAX_VALUE_INTEGER) > 0)
                {
                    throw new TypeException(ExceptionMessages.Type.OUT_OF_RANGLE_DOWNCAST_CONVERION,
                                            ((ScalarValue) source_value).toString(), TypeEnum.INTEGER.getName());
                }
                
                return new IntegerValue(((DecimalValue) source_value).getObject().intValue());
            }
        }),

        DECIMAL_TO_STRING(DecimalType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((DecimalValue) source_value).getObject().toString());
            }
        }),

        DATE_TO_DATE(DateType.class, DateType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        DATE_TO_STRING(DateType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((DateValue) source_value).getObject().toString());
            }
        }),

        TIMESTAMP_TO_TIMESTAMP(TimestampType.class, TimestampType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        TIMESTAMP_TO_STRING(TimestampType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((TimestampValue) source_value).getObject().toString());
            }
        }),

        BOOLEAN_TO_BOOLEAN(BooleanType.class, BooleanType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        BOOLEAN_TO_STRING(BooleanType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((BooleanValue) source_value).getObject().toString());
            }
        }),

        XHTML_TO_XHTML(XhtmlType.class, XhtmlType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        }),

        XHTML_TO_STRING(XhtmlType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new StringValue(((XhtmlValue) source_value).toString());
            }
        }),

        STRING_TO_DECIMAL(StringType.class, DecimalType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new DecimalValue(new BigDecimal(((StringValue) source_value).getObject()));
                }
                catch (NumberFormatException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.DECIMAL.getName());
                }
            }
        }),

        STRING_TO_DOUBLE(StringType.class, DoubleType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new DoubleValue(Double.valueOf(((StringValue) source_value).getObject()));
                }
                catch (NumberFormatException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.DOUBLE.getName());
                }
            }
        }),

        STRING_TO_FLOAT(StringType.class, FloatType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new FloatValue(Float.valueOf(((StringValue) source_value).getObject()));
                }
                catch (NumberFormatException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.FLOAT.getName());
                }
            }
        }),

        STRING_TO_LONG(StringType.class, LongType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new LongValue(Long.valueOf(((StringValue) source_value).getObject()));
                }
                catch (NumberFormatException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.LONG.getName());
                }
            }
        }),

        STRING_TO_INTEGER(StringType.class, IntegerType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new IntegerValue(Integer.valueOf(((StringValue) source_value).getObject()));
                }
                catch (NumberFormatException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.INTEGER.getName());
                }
            }
        }),

        STRING_TO_DATE(StringType.class, DateType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new DateValue(Date.valueOf(((StringValue) source_value).getObject()));
                }
                catch (IllegalArgumentException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.DATE.getName());
                }
            }
        }),

        STRING_TO_TIMESTAMP(StringType.class, TimestampType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value) throws TypeException
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                try
                {
                    return new TimestampValue(Timestamp.valueOf(((StringValue) source_value).getObject()));
                }
                catch (IllegalArgumentException e)
                {
                    throw new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION,
                                            ((StringValue) source_value).getObject(), TypeEnum.TIMESTAMP.getName());
                }
            }
        }),

        STRING_TO_BOOLEAN(StringType.class, BooleanType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new BooleanValue(Boolean.valueOf(((StringValue) source_value).getObject()));
            }
        }),

        STRING_TO_XHTML(StringType.class, XhtmlType.class, false, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                // Handle null source value
                if (source_value instanceof NullValue) return source_value;
                
                return new XhtmlValue(((StringValue) source_value).getObject());
            }
        }),

        STRING_TO_STRING(StringType.class, StringType.class, true, new Checker()
        {
            public boolean check(Type source, boolean implicit)
            {
                return true;
            }
        }, new Performer()
        {
            public Value convert(Value source_value)
            {
                return source_value;
            }
        });
        
        private Class<? extends Type> m_source;
        
        private Class<? extends Type> m_target;
        
        private boolean               m_implicit;
        
        private Checker               m_checker;
        
        private Performer             m_performer;
        
        /**
         * Constructor.
         * 
         * @param source
         *            the source type class of the conversion.
         * @param target
         *            the target type class of the conversion.
         * @param implicit
         *            whether the conversion can be implicit.
         * @param checker
         *            a secondary type conversion checker especially needed for complex types.
         * @param performer
         *            a type conversion performer.
         */
        TypeConversion(Class<? extends Type> source, Class<? extends Type> target, boolean implicit, Checker checker,
                Performer performer)
        {
            m_source = source;
            m_target = target;
            m_implicit = implicit;
            m_checker = checker;
            m_performer = performer;
        }
        
        /**
         * Gets the source type.
         * 
         * @return the source type class of the conversion.
         */
        protected Class<? extends Type> getSource()
        {
            return m_source;
        }
        
        /**
         * Gets the target type.
         * 
         * @return the target type class of the conversion.
         */
        protected Class<? extends Type> getTarget()
        {
            return m_target;
        }
        
        /**
         * Gets the secondary type conversion checker.
         * 
         * @return the secondary type conversion checker.
         */
        protected Checker getChecker()
        {
            return m_checker;
        }
        
        /**
         * Determines whether the conversion can be implicit.
         * 
         * @return true, if the conversion can be implicit; otherwise, false.
         */
        protected boolean isImplicit()
        {
            return m_implicit;
        }
        
        /**
         * Gets the type conversion performer.
         * 
         * @return the type conversion performer.
         */
        protected Performer getPerformer()
        {
            return m_performer;
        }
    }
    
    /**
     * Hidden constructor.
     */
    private TypeConverter()
    {
        m_conversion_map = new HashMap<Pair<String, String>, TypeConversion>();
        
        for (TypeConversion conversion : TypeConversion.values())
        {
            String source_name = conversion.getSource().getName();
            String target_name = conversion.getTarget().getName();
            
            Pair<String, String> pair = new Pair<String, String>(source_name, target_name);
            m_conversion_map.put(pair, conversion);
        }
    }
    
    /**
     * Gets a new instance.
     * 
     * @return a new instance.
     */
    public static TypeConverter getInstance()
    {
        return INSTANCE;
    }
    
    /**
     * Determines if a given source type class can be explicitly converted to the given target class.
     * 
     * @param source
     *            the source type of the conversion.
     * @param target
     *            the target type of the conversion.
     * @return true, if the explicit conversion is supported by the CAST function; otherwise, false.
     */
    public boolean canExplicitlyConvert(Type source, Type target)
    {
        // Handle null type
        if (source instanceof NullType) return true;
        
        Class<? extends Type> source_scalar = source.getClass();
        Class<? extends Type> target_scalar = target.getClass();
        
        Pair<String, String> pair = new Pair<String, String>(source_scalar.getName(), target_scalar.getName());
        
        if (!m_conversion_map.containsKey(pair)) return false;
        
        TypeConversion conversion = m_conversion_map.get(pair);
        Checker checker = conversion.getChecker();
        
        return checker.check(source, false);
    }
    
    /**
     * Determines if a given source type class can be implicitly converted to the given target class.
     * 
     * @param source
     *            the source type of the conversion.
     * @param target
     *            the target type of the conversion.
     * @return true, if the implicit conversion is supported by the CAST function; otherwise, false.
     */
    public boolean canImplicitlyConvert(Type source, Type target)
    {
        // Handle null type
        if (source instanceof NullType) return true;
        
        Class<? extends Type> source_scalar = source.getClass();
        Class<? extends Type> target_scalar = target.getClass();
        
        Pair<String, String> pair = new Pair<String, String>(source_scalar.getName(), target_scalar.getName());
        
        if (!m_conversion_map.containsKey(pair)) return false;
        
        TypeConversion conversion = m_conversion_map.get(pair);
        Checker checker = conversion.getChecker();
        
        return checker.check(source, true) && conversion.isImplicit();
    }
    
    /**
     * Determines if a given source type class can be implicitly converted to the given target class.
     * 
     * @param source
     *            the source type of the conversion.
     * @param target
     *            the target type of the conversion.
     * @return true, if the implicit conversion is supported by the CAST function; otherwise, false.
     */
    public boolean canImplicitlyConvert(Class<? extends Type> source, Class<? extends Type> target)
    {
        // Handle null type
        if (source == NullType.class) return true;
        
        // FIXME only handles scalar type conversion for now.
        if (!(TypeEnum.get(source) instanceof ScalarType) || !(TypeEnum.get(target) instanceof ScalarType)) return false;
        
        Pair<String, String> pair = new Pair<String, String>(source.getName(), target.getName());
        if (!m_conversion_map.containsKey(pair)) return false;
        TypeConversion conversion = m_conversion_map.get(pair);
        return conversion.isImplicit();
    }
    
    /**
     * Converts the input value to the given target type.
     * 
     * @param source_value
     *            the source value of the conversion.
     * @param target_type
     *            the target type of the conversion.
     * @return the converted scalar value..
     * @throws TypeException
     *             if there is a type exception during conversion.
     */
    public Value convert(Value source_value, Type target_type) throws TypeException
    {
        // Handle null source value
        if (source_value instanceof NullValue)
        {
            return source_value;
        }
        
        Pair<String, String> pair = new Pair<String, String>(source_value.getTypeClass().getName(),
                                                             target_type.getClass().getName());
        
        TypeConversion conversion = m_conversion_map.get(pair);
        
        // Invalid type conversion
        if (conversion == null)
        {
            throw new TypeException(ExceptionMessages.Type.INVALID_TYPE_CONVERION, source_value.getTypeClass().getName(),
                                    target_type.getClass().getName());
        }
        
        return conversion.getPerformer().convert(source_value);
    }
    
    /**
     * Checks if the source collection type can be converted to the target scalar type.
     * 
     * @param source
     *            the source collection type.
     * @param target
     *            the target scalar type.
     * @param implicit
     *            whether the check is for an implicit conversion.
     * @return true, if the source collection type can be converted to the target scalar type, otherwise, false.
     */
    private boolean checkCollectionToScalar(CollectionType source, Type target, boolean implicit)
    {
        assert (target instanceof ScalarType);
        
        return TypeConverter.getInstance().checkTupleToScalar(source.getTupleType(), target, implicit);
    }
    
    /**
     * Checks if the source tuple type can be converted to the target scalar type.
     * 
     * @param source
     *            the source tuple type.
     * @param target
     *            the target scalar type.
     * @param implicit
     *            whether the check is for an implicit conversion.
     * @return true, if the source tuple type can be converted to the target scalar type, otherwise, false.
     */
    private boolean checkTupleToScalar(TupleType source, Type target, boolean implicit)
    {
        assert (target instanceof ScalarType);
        
        if (source.getSize() < 1) return false;
        
        String attr_name = source.getAttributeNames().iterator().next();
        
        return (implicit)
                ? TypeConverter.getInstance().canImplicitlyConvert(source.getAttribute(attr_name), target)
                : TypeConverter.getInstance().canExplicitlyConvert(source.getAttribute(attr_name), target);
    }
    
    /**
     * Converts the input collection value into the target scalar type.
     * 
     * @param source_value
     *            a source collection value.
     * @param target_type
     *            a target scalar type.
     * @return the converted scalar value.
     * @throws TypeException
     *             if there is a type exception during conversion.
     */
    private Value convertCollectionToScalar(Value source_value, Type target_type) throws TypeException
    {
        assert (target_type instanceof ScalarType);
        
        // Handle NULL source value
        if (source_value instanceof NullValue)
        {
            return new NullValue(target_type.getClass());
        }
        
        CollectionValue coll_value = (CollectionValue) source_value;
        
        // Handle empty collection
        if (coll_value.size() == 0)
        {
            return new NullValue(target_type.getClass());
        }
        else if (coll_value.size() == 1)
        {
            Value converted = TypeConverter.getInstance().convertTupleToScalar(coll_value.getTuples().get(0), target_type);
            
            return converted;
        }
        
        throw new TypeException(ExceptionMessages.Type.INVALID_COLLECTION_TO_SCALAR_CONVERION);
    }
    
    /**
     * Converts the input tuple value into the target scalar type.
     * 
     * @param source_value
     *            a source tuple value.
     * @param target_type
     *            a target scalar type.
     * @return the converted scalar value.
     * @throws TypeException
     *             if there is a type exception during conversion.
     */
    private Value convertTupleToScalar(Value source_value, Type target_type) throws TypeException
    {
        assert (target_type instanceof ScalarType);
        
        // Handle NULL source value
        if (source_value instanceof NullValue)
        {
            return new NullValue(target_type.getClass());
        }
        
        TupleValue tuple_value = (TupleValue) source_value;
        
        // Handle empty tuple
        if (tuple_value.getSize() == 0)
        {
            return new NullValue(target_type.getClass());
        }
        if (tuple_value.getSize() == 1)
        {
            String attr_name = tuple_value.getAttributeNames().iterator().next();
            
            Value converted = TypeConverter.getInstance().convert(tuple_value.getAttribute(attr_name), target_type);
            
            return converted;
        }
        
        throw new TypeException(ExceptionMessages.Type.INVALID_TUPLE_TO_SCALAR_CONVERION);
    }
}
