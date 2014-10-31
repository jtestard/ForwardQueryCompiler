CREATE DATA OBJECT session.credentials TUPLE(
    user string,
    roles collection (
        role string,
        PRIMARY KEY(role)
    )
)