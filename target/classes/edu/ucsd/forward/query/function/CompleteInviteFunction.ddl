CREATE FUNCTION complete_invite
(
    input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        invitation_token_attribute string, /* invitation token attribute */
        active_attribute string, /* active attribute */
        password_attribute string, /* password attribute */

        /* Values to process */
        invitation_token string, /* invitation token */
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