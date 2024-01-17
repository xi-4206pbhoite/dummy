# Package the Lambda function
zip -r ./hello_world.zip codebase/

# Deploy the Lambda function to AWS
#aws lambda update-function-code --function-name lambda_helloworld --zip-file fileb://./hello_world.zip

aws lambda create-function \
  --function-name lambda_handler \
  --runtime python3.8 \
  --role arn:aws:iam::474532148129:role/Lambda-Role \
  --zip-file fileb://hello_world.zip \
  --handler codebase/hello_world.lambda_handler \
  --region ap-south-1
