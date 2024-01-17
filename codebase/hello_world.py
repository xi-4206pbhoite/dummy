def lambda_helloworld(event, context):
    return {
        'statusCode': 200,
        'body': 'Hello, World!'
    }

def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': 'Hello, Lambda!'
    }