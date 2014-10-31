CREATE FUNCTION generate_auth_token
(input tuple (
        /* Information about the table and fields */
        data_source string, /* JDBC Data source name */
        data_object string, /* Data object or table name */
        username_attribute string, /* username attribute */
        token_attribute string, /* token attribute */
        remaining_uses_attribute string, /* (optional) attribute for number of remaining uses */
        next_url_attribute string, /* (not needed if store_next_url is false) attribute to store next url */
        expiry_timestamp_attribute string, /* (optional) attribute to store expiry timestamp */
        /* Values to process or insert into the database */
        username string, /* username */
        next_url string, /* next url */
        store_next_url boolean, /* whether to store the url in the database. If false or null, the output query string will contain the next url */
        remaining_uses integer, /* number of remaining uses. null means unlimited */
        days_to_expire integer /* number of days to expiry. null means never */
    )
 )
RETURNS switch(
        success tuple(
            query_string string /* "token=xyz" or "token=xyz&next_url=[url] */
        ),
        failure tuple(
            error string /* error message */
        )
    )
AS BEGIN	
END; 