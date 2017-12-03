# dynamodb-meta-store
Store and update metadata in DynamoDB

# Install
    pip install dynamodb_meta_store 

# Usage
    store = DynamoDBMetaStore(
        'us-east-1',                # AWS region to connect to
        table_name='test',          # DynamoDB table name
        store_name='store')         # Store name

    # Set the 'graylog' metadata object
    obj = {
        'host': '127.0.0.1',
        'port': 12201
    }
    store.set('graylog', obj)
    # Returns: True

    # Get the 'graylog' metadata object
    store.get('graylog')
    # Returns: {'host': '127.0.0.1', 'port': 12201}


# Credits
This repo is inspired and based on https://github.com/sebdah/dynamodb-config-store