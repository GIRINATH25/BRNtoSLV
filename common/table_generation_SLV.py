from sqlalchemy import text

DATA_TYPE_MAP = {
    "INT": {"mysql": "INT", "postgresql": "INTEGER", "mssql": "INT"},
    "VARCHAR": {"mysql": "VARCHAR", "postgresql": "VARCHAR", "mssql": "VARCHAR"},
    "DECIMAL": {"mysql": "DECIMAL", "postgresql": "NUMERIC", "mssql": "DECIMAL"},
    "TEXT": {"mysql": "TEXT", "postgresql": "TEXT", "mssql": "TEXT"},
    "DATE": {"mysql": "DATE", "postgresql": "DATE", "mssql": "DATE"},
    "TIMESTAMP WITHOUT TIME ZONE": {"mysql": "TIMESTAMP", "postgresql": "TIMESTAMP", "mssql": "DATETIME"},
    "TIMESTAMP": {"mysql": "TIMESTAMP", "postgresql": "TIMESTAMP", "mssql": "DATETIME"}
}

DEFAULT_COLUMNS = [
    ("etlactiveind", "INT"),
    ("etljobname", "VARCHAR(200)"),
    ("envsourcecd", "VARCHAR(50)"),
    ("datasourcecd", "VARCHAR(50)"),
    ("etlcreateddatetime", "TIMESTAMP"),
    ("etlupdateddatetime", "TIMESTAMP")
]

def detect_database_type(engine):
    """Detect the database type using SQLAlchemy engine dialect."""
    db_type = engine.dialect.name.lower()

    if db_type == "postgresql":
        return "postgresql"
    elif db_type == "mysql":
        return "mysql"
    elif db_type in {"mssql", "mssql+pyodbc"}:
        return "mssql"
    else:
        raise ValueError(f"Unsupported Database: {db_type}")

def fetch_table_metadata(engine, table_name):
    """Fetch metadata for the given table from INFORMATION_SCHEMA."""
    query = f"""
    SELECT 
        c.COLUMN_NAME AS column_name,
        c.DATA_TYPE AS data_type,
        COALESCE(c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION) AS max_length,
        c.NUMERIC_PRECISION AS precisions,
        c.NUMERIC_SCALE AS scale,
        c.IS_NULLABLE AS is_nullable,
        tc.CONSTRAINT_TYPE AS constraint_type,
        tc.CONSTRAINT_NAME AS constraint_name
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
        ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE c.TABLE_NAME = '{table_name}';
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

def generate_create_table_sql(engine, source_table_name, target_table_name):
    """Generate an accurate CREATE TABLE SQL statement for the detected DB."""
    metadata = fetch_table_metadata(engine, source_table_name)

    if not metadata:
        print(f"No metadata found for table: {source_table_name}")
        return None

    db_type = detect_database_type(engine)
    sql = f"CREATE TABLE {target_table_name} (\n"
    primary_keys = []

    for row in metadata:
        col_name = row.column_name
        raw_data_type = row.data_type.upper().replace(" WITHOUT TIME ZONE", "")  
        data_type = DATA_TYPE_MAP.get(raw_data_type, {}).get(db_type, raw_data_type)

        max_length = f"({row.max_length})" if row.max_length and "CHAR" in raw_data_type else ""
        is_nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"

        auto_increment = ""
        if row.constraint_type == "PRIMARY KEY":
            primary_keys.append(col_name)
            if db_type == "mysql":
                auto_increment = "AUTO_INCREMENT"
            elif db_type == "postgresql":
                data_type = "SERIAL"
                max_length = ""  
            elif db_type == "mssql":
                auto_increment = "IDENTITY(1,1)"

        if max_length == "" and db_type == 'mysql' and "CHAR" in raw_data_type:
            data_type = "TEXT"

        col_def = f"    {col_name} {data_type}{max_length} {auto_increment} {is_nullable}".strip()
        sql += col_def + ",\n"

    for col_name, col_type in DEFAULT_COLUMNS:
        sql += f"    {col_name} {col_type} NULL,\n"

    if primary_keys:
        sql += f"    PRIMARY KEY ({', '.join(primary_keys)})\n"
    else:
        last = f"    {target_table_name[2:]}key BIGINT "
        if db_type == "mysql":
            last += "AUTO_INCREMENT NOT NULL"
        elif db_type == "postgresql":
            last += "GENERATED ALWAYS AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL"
        elif db_type == "mssql":
            last += "IDENTITY(1,1) NOT NULL"

        sql += last + ",\n"
        sql += f"    PRIMARY KEY ({target_table_name[2:]}key)\n"

    sql = sql.rstrip(",\n") + "\n);"
    # print(sql)
    return sql

