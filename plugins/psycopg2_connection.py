import psycopg2


def db_connection(db_cred: dict):
    """
    Create a PostgreSQL database connection.

    Args:
        db_cred (dict): Dictionary containing database credentials:
            {
                "host": str,
                "database_name": str,
                "username": str,
                "password": str,
                "port": int (optional, default=5432)
            }

    Returns:
        connection: psycopg2 connection object
    """

    return psycopg2.connect(
        host=db_cred["host"],
        dbname=db_cred["database_name"],
        user=db_cred["username"],
        password=db_cred["password"],
    )