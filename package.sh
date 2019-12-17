#!/bin/bash

rm -rf tmp411
mkdir -p tmp411

if [ "$TESTING" == "true" ];
then
    echo "DOING TESTING SETUP"
    python3 -m pip install -r /io/requirements.txt
    pushd /io/Lambda
    DONOTCOMPILE=1 python3 setup.py develop
    popd
    pushd /io/Lambda
    DONOTCOMPILE=1 python3 setup.py develop
    popd
else
    echo "DOING PROD SETUP"
    python3 -m pip install -r /io/requirements.txt -t tmp411
    python3 -m pip install /io/Lambda/ -t tmp411
    DONOTCOMPILE=1 python3 -m pip install /io/Lambda/ -t tmp411

    # Untested optimization to reduce deployment size
    # See https://github.com/ralienpp/simplipy/blob/master/README.md
    #echo DELETING *.py `find tmp411 -name "*.py" -type f`
    #find tmp411 -name "*.py" -type f -delete

    # remove any old .zips
    rm -f /io/lambda.zip

    # grab existing products
    cp -r /io/* tmp411

    # zip without any containing folder (or it won't work)
    cd tmp411
    zip -r /io/lambda.zip *
fi