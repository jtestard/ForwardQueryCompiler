CREATE FUNCTION verify_auth_token
(
    input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        username_attribute string, /* username attribute */
        token_attribute string, /* token attribute */
        remaining_uses_attribute string, /* (optional) attribute for number of remaining uses */
        next_url_attribute string, /* (not needed if store_next_url is false) attribute to store next url */
        expiry_timestamp_attribute string, /* (optional) attribute to store expiry timestamp */
        /* Values to process */
        token string, /* token for looking up entry */
        next_url string /* not needed if next url is retrieved from table. Otherwise, this value is returned in the output. */
    )
)
RETURNS    switch(
        success tuple(
            next_url string
        ),
        failure tuple(
            error string
        )
    )  
AS BEGIN
END;