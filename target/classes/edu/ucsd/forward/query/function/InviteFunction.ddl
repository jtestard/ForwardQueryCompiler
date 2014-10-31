CREATE FUNCTION invite
(
    input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        username_attribute string, /* username attribute */
        invitation_token_attribute string, /* invitation token attribute */
        active_attribute string, /* active attribute */

        /* Values to process */
        username string
    )
)
RETURNS switch(
        success tuple(
            invitation_token string
        ),
        failure tuple(
            error string
        )
    )
AS BEGIN	
END; 