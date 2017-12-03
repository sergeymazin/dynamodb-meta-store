# dynamodb-meta-store
Store and update metadata in DynamoDB

# Install
    pip install dynamodb_meta_store 

# Usage
    store = DynamoDBMetaStore(
        table_name='test',          # DynamoDB table name
        store_name='infra')         # Store name

    or
    
    import boto3
    conn = boto3.resource('dynamodb', 'us-west-1')
    store = DynamoDBMetaStore(
        table_name='test',          # DynamoDB table name
        store_name='infra',         # Store name
        connection=conn)            # Connection to DynamoDB resource


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