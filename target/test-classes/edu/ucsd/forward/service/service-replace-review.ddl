CREATE DATA OBJECT service.replace_review
tuple (
    input collection (
        proposal integer,
        reviewer string,
        comment string,
        depth integer,
        impact integer,
        overall integer,
        PRIMARY KEY(proposal, reviewer)
    ),
    outcome switch (
        success tuple (
            no_affected_tuples integer
        ),
        failure tuple (
            failure_message string
        )
    )
)