
if [ -z "$USER" ]; then
    echo "assign value to USER to upload data to" 
    exit 1
fi

if [ -z "$PASSWORD" ]; then
    echo "assign value to PASSWORD to upload data to" 
    exit 1
fi

if [ -z "$ACCOUNT" ]; then
    echo "assign value to ACCOUNT to upload data to" 
    exit 1
fi

if [ -z "$DATABASE" ]; then
    echo "assign value to DATABASE to upload data to" 
    exit 1
fi

if [ -z "$SCHEMA" ]; then
    echo "assign value to SCHEMA to upload data to" 
    exit 1
fi

if [ -z "$WAREHOUSE" ]; then
    echo "assign value to WAREHOUSE to upload data to" 
    exit 1
fi

python -m src.loader --user "$USER" --password "$PASSWORD" --account "$ACCOUNT" --database "$DATABASE" --schema "$SCHEMA" --warehouse "$WAREHOUSE"