#!/bin/bash

DOCKER_USERNAME=$DOCKER_USERNAME
DOCKER_PASSWORD=$DOCKER_PASSWORD
PROJECT_NAME=$CIRCLE_PROJECT_REPONAME
CIRCLE_TAG=${CIRCLE_TAG}

IMAGE_TAG="elasticio/${PROJECT_NAME}:${CIRCLE_TAG}"

# -e Exit immediately if a command returns a non-zero status
# -u Treat unset variables and parameters as an error
# -o pipefail Return value of a pipeline is the value of the last command with non-zero status
set -euo pipefail

git archive master | docker run -i -a stdin -a stdout -a stderr --env LOG_OUTPUT_MODE=long elasticio/appbuilder:2.0.1 - > slug.tgz

mkdir -p ./slug && tar -xf slug.tgz -C ./slug
cd ./slug
COMPONENT=$(node -p "JSON.stringify(require('./component.json'))")

echo $DOCKER_PASSWORD | docker login -u $DOCKER_USERNAME --password-stdin
docker build \
           --tag "${IMAGE_TAG}" \
           --label elastic.io.repoId="${PROJECT_NAME}" \
           --label elastic.io.buildId="${CIRCLE_TAG}" \
           --label elastic.io.hash="${CIRCLE_SHA1}" \
           --label elastic.io.component="${COMPONENT}" \
           -f ../.circleci/Dockerfile .

docker push "${IMAGE_TAG}"
