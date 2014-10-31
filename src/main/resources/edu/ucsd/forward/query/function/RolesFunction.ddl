CREATE FUNCTION roles
(    
   username string    
)
RETURNS    switch(
        success tuple(
            roles collection (
                role string,
                primary key (role)
            )
        ),
        failure tuple(
            error string
        )
    )  
AS BEGIN
END;
