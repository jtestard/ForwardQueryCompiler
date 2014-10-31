CREATE FUNCTION authenticate
(
    input tuple (
        username string,
        password string
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