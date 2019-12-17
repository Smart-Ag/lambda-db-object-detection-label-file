#!/bin/bash

echo    # (optional) move to a new line

if [[ $SKIP_PROMPT =~ ^[Yy]$ ]]
then
    if [ "$TRAVIS_BRANCH" == "prod" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ];
    then
        # do dangerous stuff
        echo Pushing to s3
        aws s3 cp lambda.zip s3://smart-ag-lambda-code-deploy-us-east-1-v2/lambda-update-insert_dynamodb-object-detection-label-file/lambda-update-insert_dynamodb-object-detection-label-file.zip
        echo Doing Prod Deployment
        aws lambda update-function-code \
            --function-name update_insert_dynamodb_object_detection_label_file \
            --s3-bucket smart-ag-lambda-code-deploy-us-east-1-v2 \
            --s3-key lambda-update-insert_dynamodb-object-detection-label-file/lambda-update-insert_dynamodb-object-detection-label-file.zip \
            --region us-east-1

        echo Done With Prod Deployment
    else
        echo skipping deploy
        echo TRAVIS_BRANCH is $TRAVIS_BRANCH
        echo TRAVIS_PULL_REQUEST is $TRAVIS_PULL_REQUEST
    fi
else
    echo Skipping Until You Confirm For Your Protection
fi