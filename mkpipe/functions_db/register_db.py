DB_CONNECTIONS = {}


def register_db_connector(db_type):
    """
    Decorator to register a database connector function under a specified database type.

    Parameters:
        db_type (str): The type of the database (e.g., 'postgresql').

    Returns:
        decorator: A decorator that registers the function in the DB_CONNECTIONS dictionary.
    """

    def decorator(fn):
        DB_CONNECTIONS[db_type] = fn
        return fn

    return decorator


def get_db_connector(db_type):
    """
    Retrieve the appropriate database connector function based on the database type.

    Parameters:
        db_type (str): The type of the database (e.g., 'postgresql').

    Returns:
        function: The connector function for the specified database type.

    Raises:
        ValueError: If the specified database type is not registered.
    """
    if db_type not in DB_CONNECTIONS:
        raise ValueError(f'Unsupported database type: {db_type}')
    return DB_CONNECTIONS[db_type]
