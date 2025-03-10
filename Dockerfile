FROM node:slim

WORKDIR /app

ADD . .

RUN npm install
RUN npm run build

# Default to ClickHouse and SQS
ENV DB_TYPE=CLICKHOUSE
ENV QUEUE_TYPE=SQS

CMD ["npm", "start"]
