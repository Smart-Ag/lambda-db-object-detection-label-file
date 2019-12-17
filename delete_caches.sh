#!/bin/bash
rm -rf .cache/
rm -rf Lambda/lambda_update.egg-info/
rm -rf Lambda/tests/__pycache__
rm -rf Lambda/tests/unit_tests/__pycache__
rm -rf Lambda/.pytest_cache
rm -rf Lambda/update/__pycache__
