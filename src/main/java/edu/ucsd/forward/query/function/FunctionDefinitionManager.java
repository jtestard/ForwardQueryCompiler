/**
 * 
 */
package edu.ucsd.forward.query.function;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * Manages the definitions for the built-in functions.
 * 
 * @author Yupeng
 * 
 */
public final class FunctionDefinitionManager
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FunctionDefinitionManager.class);
    
    private FunctionDefinitionManager()
    {
    }
    
    public static String getAuthenticateFunction()
    {
        return get("AuthenticateFunction.ddl");
    }
    
    public static String getCompletePasswordResetFunction()
    {
        return get("CompletePasswordResetFunction.ddl");
    }
    
    public static String getPasswordResetFunction()
    {
        return get("PasswordResetFunction.ddl");
    }
    
    public static String getCompleteSignUpFunction()
    {
        return get("CompleteSignUpFunction.ddl");
    }
    
    public static String getSignUpFunction()
    {
        return get("SignUpFunction.ddl");
    }
    
    public static String getVerifyAuthTokenFunction()
    {
        return get("VerifyAuthTokenFunction.ddl");
    }
    
    public static String getGenerateAuthTokenFunction()
    {
        return get("GenerateAuthTokenFunction.ddl");
    }
    
    public static String getRolesFunction()
    {
        return get("RolesFunction.ddl");
    }
    
    public static String getInviteFunction()
    {
        return get("InviteFunction.ddl");
    }
    
    public static String getCompleteInviteFunction()
    {
        return get("CompleteInviteFunction.ddl");
    }
    
    public static String getResumeActionFunction()
    {
        return get("ResumeActionFunction.ddl");
    }
    
    public static String getCheckAccessFunction()
    {
        return get("CheckAccessFunction.ddl");
    }
    
    public static String getOutOfMemoryFunction()
    {
        return get("OutOfMemory.ddl");
    }    
    
    private static String get(String path)
    {
        return IoUtil.getResourceAsString(FunctionDefinitionManager.class, path);
    }
}
