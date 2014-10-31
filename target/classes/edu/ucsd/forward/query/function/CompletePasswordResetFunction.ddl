CREATE FUNCTION complete_password_reset
(
    input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        password_reset_token_attribute string, /* password reset token attribute */
        password_attribute string, /* password attribute */

        /* Values to process */
        password_reset_token string, /* password reset token */
        password string /* password to set */
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