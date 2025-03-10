FROM node:slim

WORKDIR /app

ADD . .

RUN npm install
RUN npm run build

# Default to ClickHouse and SQS
ENV DB_TYPE=CLICKHOUSE
ENV QUEUE_TYPE=SQS
ENV AWS_REGION=dummy

ENV NODE_ENV=production
CMD ["npm", "start"]
