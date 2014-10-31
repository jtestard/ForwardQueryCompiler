CREATE FUNCTION complete_signup
(
    input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        activation_token_attribute string, /* activation token attribute */
        active_attribute string, /* active attribute */
        /* Values to process */
        activation_token string /* activation token */
    )
)
RETURNS switch(
        success tuple(
        ),
        failure tuple(
            error string
        )
    )
AS BEGIN
	
END ;