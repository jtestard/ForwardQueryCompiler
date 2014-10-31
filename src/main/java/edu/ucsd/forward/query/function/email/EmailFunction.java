/**
 * 
 */
package edu.ucsd.forward.query.function.email;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The email function that sends email.
 * 
 * @author Yupeng
 * 
 */
public class EmailFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log           = Logger.getLogger(EmailFunction.class);
    
    public static final String  NAME          = "EMAIL";
    
    private static final String SMTP_SERVER   = "smtp_server";
    
    private static final String HOST          = "host";
    
    private static final String PORT          = "port";
    
    private static final String USER_NAME     = "user_name";
    
    private static final String PASSWORD      = "password";
    
    private static final String TLS           = "tls";
    
    private static final String FROM_ADDRESS  = "from_address";
    
    private static final String TO_ADDRESS    = "to_address";
    
    private static final String TO_ADDRESSES  = "to_addresses";
    
    private static final String SUBJECT       = "subject";
    
    private static final String BODY          = "body";
    
    private static final String ERROR_MESSAGE = "error_message";
    
    private static final String SUCCESSFUL    = "successful";
    
    /**
     * The signatures of the function.
     * 
     */
    private enum FunctionSignatureName
    {
        EMAIL;
    }
    
    /**
     * The default constructor.
     */
    public EmailFunction()
    {
        super(NAME);
        m_data_source = DataSource.MEDIATOR;
        
        FunctionSignature signature;
        
        String config = "email";
        TupleType return_type = new TupleType();
        return_type.setAttribute(SUCCESSFUL, new BooleanType());
        return_type.setAttribute(ERROR_MESSAGE, new StringType());
        
        signature = new FunctionSignature(FunctionSignatureName.EMAIL.name(), return_type);
        TupleType arg_type = new TupleType();
        TupleType smtp_server_type = new TupleType();
        arg_type.setAttribute(SMTP_SERVER, smtp_server_type);
        smtp_server_type.setAttribute(HOST, new StringType());
        smtp_server_type.setAttribute(PORT, new IntegerType());
        smtp_server_type.setAttribute(USER_NAME, new StringType());
        smtp_server_type.setAttribute(PASSWORD, new StringType());
        smtp_server_type.setAttribute(TLS, new BooleanType());
        arg_type.setAttribute(FROM_ADDRESS, new StringType());
        TupleType to_address_type = new TupleType();
        to_address_type.setAttribute(TO_ADDRESS, new StringType());
        arg_type.setAttribute(TO_ADDRESSES, new CollectionType(to_address_type));
        arg_type.setAttribute(SUBJECT, new StringType());
        arg_type.setAttribute(BODY, new StringType());
        signature.addArgument(config, arg_type);
        this.addFunctionSignature(signature);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        TupleValue email_tuple = (TupleValue) evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        
        // Get SMTP server configuration
        TupleValue smtp_server_tuple = (TupleValue) email_tuple.getAttribute("smtp_server");
        String smtp_host = ValueUtil.getString(smtp_server_tuple, "host");
        Integer smtp_port = ValueUtil.getInteger(smtp_server_tuple, "port");
        String smtp_user_name = ValueUtil.getString(smtp_server_tuple, "user_name");
        String smtp_password = ValueUtil.getString(smtp_server_tuple, "password");
        Boolean smtp_tls = ValueUtil.getBoolean(smtp_server_tuple, "tls");
        
        // Get email headers
        String from_address = ValueUtil.getString(email_tuple, "from_address");
        CollectionValue to_addresses_collection = (CollectionValue) email_tuple.getAttribute("to_addresses");
        List<String> to_addresses = new ArrayList<String>();
        for (TupleValue to_address_tuple : to_addresses_collection.getTuples())
        {
            to_addresses.add(ValueUtil.getString(to_address_tuple, "to_address"));
        }
        String subject = ValueUtil.getString(email_tuple, "subject");
        
        // TODO: Add attributes cc_addresses and bcc_addresses when DDLs can provide defaults for collections
        
        // Get email body
        String body = ValueUtil.getString(email_tuple, "body");
        
        // HACK: Query parser does not support newline escapes (T173)
        body = body.replaceAll("\\\\n", "\n");
        
        // Use Apache Commons Email, which is more convenient than the Java Mail API
        String error = null;
        try
        {
            Email email = new SimpleEmail();
            email.setHostName(smtp_host);
            email.setSmtpPort(smtp_port);
            email.setAuthenticator(new DefaultAuthenticator(smtp_user_name, smtp_password));
            email.setTLS(smtp_tls);
            email.setFrom(from_address);
            for (String to_address : to_addresses)
            {
                email.addTo(to_address);
            }
            email.setSubject(subject);
            email.setMsg(body);
            email.send();
        }
        catch (EmailException e)
        {
            // Get the entire stack trace as a string, as there may be chained exceptions
            StringWriter string_writer = new StringWriter();
            error = string_writer.toString();
            throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e);
        }
        
        TupleValue return_value = new TupleValue();
        return_value.setAttribute(SUCCESSFUL, new BooleanValue(error == null));
        
        // Return different outcomes based on whether an error occured while sending the email
        if (error == null)
        {
            return_value.setAttribute(ERROR_MESSAGE, new NullValue());
        }
        else
        {
            return_value.setAttribute(ERROR_MESSAGE, new StringValue(error));
        }
        return new BindingValue(return_value, false);
    }
}
