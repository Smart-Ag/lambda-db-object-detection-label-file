#!/bin/bash

echo    # (optional) move to a new line

if [[ $SKIP_PROMPT =~ ^[Yy]$ ]]
then
    if [ "$TRAVIS_BRANCH" == "prod" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ];
    then
        # do dangerous stuff
        echo Pushing to s3
        aws s3 cp lambda.zip s3://smart-ag-lambda-code-deploy-us-east-1-v2/update_insert_s3_object_detection_label_file/update_insert_s3_object_detection_label_file.zip
        echo Doing Prod Deployment 1
        aws lambda update-function-code \
            --function-name update_insert_s3_object_detection_label_file \
            --s3-bucket smart-ag-lambda-code-deploy-us-east-1-v2 \
            --s3-key update_insert_s3_object_detection_label_file/update_insert_s3_object_detection_label_file.zip \
            --region us-east-1
        echo Done With Prod Deployment 1

        echo Doing Prod Deployment 2
        aws lambda update-function-code \
            --function-name update_insert_s3_object_detection_img_file \
            --s3-bucket smart-ag-lambda-code-deploy-us-east-1-v2 \
            --s3-key update_insert_s3_object_detection_label_file/update_insert_s3_object_detection_label_file.zip \
            --region us-east-1
        echo Done With Prod Deployment 2

        echo Doing Prod Deployment 3
        aws lambda update-function-code \
            --function-name delete_delete_s3_object_detection_label_file \
            --s3-bucket smart-ag-lambda-code-deploy-us-east-1-v2 \
            --s3-key update_insert_s3_object_detection_label_file/update_insert_s3_object_detection_label_file.zip \
            --region us-east-1
        echo Done With Prod Deployment 3
    else
        echo skipping deploy
        echo TRAVIS_BRANCH is $TRAVIS_BRANCH
        echo TRAVIS_PULL_REQUEST is $TRAVIS_PULL_REQUEST
    fi
else
    echo Skipping Until You Confirm For Your Protection
fi