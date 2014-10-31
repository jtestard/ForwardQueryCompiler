CREATE FUNCTION signup
(
    input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        username_attribute string, /* username attribute */
        password_attribute string, /* password attribute */
        activation_token_attribute string, /* activation token attribute */
        active_attribute string, /* active attribute */

        /* Values to process */
        username string,
        password string
    )   
)
RETURNS switch(
        success tuple(
            activation_token string
        ),
        failure tuple(
            error string
        )
    )
AS BEGIN
	
END ;