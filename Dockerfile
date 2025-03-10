FROM node:slim

WORKDIR /app

ENV NODE_ENV=production
ADD . .

RUN npm install
RUN npm run build

# Default to ClickHouse and SQS
ENV DB_TYPE=CLICKHOUSE
ENV QUEUE_TYPE=SQS
ENV AWS_REGION=dummy

CMD ["npm", "start"]
