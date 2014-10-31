/**
 * 
 */
package edu.ucsd.forward.query.function.metadata;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * A metadata function that converts a schema object tree into a (queryable) data tree.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class SchemaObjectFunction extends AbstractMetaDataFunction
{
    @SuppressWarnings("unused")
    private static final Logger log    = Logger.getLogger(SchemaObjectFunction.class);
    
    public static final String  NAME   = "schema_object";
    
    private static final String ID     = "id";
    
    private static final String LABEL  = "label";
    
    private static final String TYPE   = "type";
    
    private static final String PARENT = "parent_id";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        SCHEMA_OBJECT;
    }
    
    /**
     * The default constructor.
     */
    public SchemaObjectFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name_one = "data_source_name";
        String arg_name_two = "schema_object_name";
        
        signature = new FunctionSignature(FunctionSignatureName.SCHEMA_OBJECT.name(), TypeEnum.COLLECTION.get());
        signature.addArgument(arg_name_one, TypeEnum.STRING.get());
        signature.addArgument(arg_name_two, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value data_source_value = arg_values.get(0);
        Value schema_object_value = arg_values.get(1);
        
        // Handle NULL arguments
        if (data_source_value instanceof NullValue || schema_object_value instanceof NullValue)
        {
            return new BindingValue(new NullValue(), true);
        }
        
        String data_source_name = ((StringValue) data_source_value).toString();
        String schema_object_name = ((StringValue) schema_object_value).toString();
        
        // Retrieve the data source access
        DataSource data_source = QueryProcessorFactory.getInstance().getDataSourceAccess(data_source_name).getDataSource();
        SchemaObject schema_object = data_source.getSchemaObject(schema_object_name);
        
        CollectionValue result = new CollectionValue();
        
        Type root_type = schema_object.getSchemaTree().getRootType();
        TupleValue root_tuple = new TupleValue();
        
        root_tuple.setAttribute(ID, new IntegerValue(1));
        root_tuple.setAttribute(LABEL, new StringValue(schema_object_name));
        root_tuple.setAttribute(TYPE, new StringValue(TypeEnum.getName(root_type)));
        root_tuple.setAttribute(PARENT, new IntegerValue(0));
        result.add(root_tuple);
        
        // Recurse
        visit(root_type, 1, result);
        
        return new BindingValue(result, true);
    }
    
    private int visit(Type type, int id, CollectionValue collection)
    {
        if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            
            return visit(collection_type.getTupleType(), id, collection);
        }
        else if (type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) type;
            int id_counter = id;
            
            for (AttributeEntry entry : tuple_type)
            {
                TupleValue tuple = new TupleValue();
                
                tuple.setAttribute(ID, new IntegerValue(id_counter + 1));
                tuple.setAttribute(LABEL, new StringValue(entry.getName()));
                tuple.setAttribute(TYPE, new StringValue(TypeEnum.getName(entry.getType())));
                tuple.setAttribute(PARENT, new IntegerValue(id));
                collection.add(tuple);
                
                id_counter = visit(entry.getType(), id_counter + 1, collection);
            }
            
            return id_counter;
        }
        else if (type instanceof SwitchType)
        {
            SwitchType switch_type = (SwitchType) type;
            int id_counter = id;
            
            for (String case_name : switch_type.getCaseNames())
            {
                TupleValue tuple = new TupleValue();
                
                tuple.setAttribute(ID, new IntegerValue(id_counter + 1));
                tuple.setAttribute(LABEL, new StringValue(case_name));
                tuple.setAttribute(TYPE, new StringValue(TypeEnum.getName(switch_type.getCase(case_name))));
                tuple.setAttribute(PARENT, new IntegerValue(id));
                collection.add(tuple);
                
                id_counter = visit(switch_type.getCase(case_name), id_counter + 1, collection);
            }
            
            return id_counter;
        }
        else
        {
            assert (type instanceof ScalarType);
            return id;
        }
    }
}
