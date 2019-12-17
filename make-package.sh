#!/bin/bash

docker run --rm -v $(pwd):/io -t \
    python-lambda36 \
	bash /io/package.sh