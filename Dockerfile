FROM node:slim

WORKDIR /app

ADD . .

ARG NPM_GITHUB_TOKEN \
    SQS_QUEUE_URL \
    QUEUE_TYPE=SQS \
    DB_TYPE=DYNAMODB \
    NUMBER_OF_WORKERS=2 \
    AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY \
    AWS_REGION=eu-north-1 \
    NODE_ENV=development

# This is needed for installing private github packages
RUN echo //npm.pkg.github.com/:_authToken=$NPM_GITHUB_TOKEN >> ~/.npmrc
RUN echo @eyevinn:registry=https://npm.pkg.github.com/ >> ~/.npmrc
RUN npm install
# Removing the github token from npmrc file in below line
RUN echo > ~/.npmrc

ENV SQS_QUEUE_URL=$SQS_QUEUE_URL \
    QUEUE_TYPE=$QUEUE_TYPE \
    DB_TYPE=$DB_TYPE \
    NUMBER_OF_WORKERS=$NUMBER_OF_WORKERS \
    AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    AWS_REGION=$AWS_REGION \
    NODE_ENV=$NODE_ENV

RUN npm run build

CMD ["npm", "start"]
