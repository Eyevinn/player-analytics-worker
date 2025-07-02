[![Slack](https://slack.osaas.io/badge.svg)](https://slack.osaas.io)

#  Eyevinn Open Analytics Worker

> _Part of Eyevinn Open Analytics Solution_

[![Badge OSC](https://img.shields.io/badge/Evaluate-24243B?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMTIiIGN5PSIxMiIgcj0iMTIiIGZpbGw9InVybCgjcGFpbnQwX2xpbmVhcl8yODIxXzMxNjcyKSIvPgo8Y2lyY2xlIGN4PSIxMiIgY3k9IjEyIiByPSI3IiBzdHJva2U9ImJsYWNrIiBzdHJva2Utd2lkdGg9IjIiLz4KPGRlZnM%2BCjxsaW5lYXJHcmFkaWVudCBpZD0icGFpbnQwX2xpbmVhcl8yODIxXzMxNjcyIiB4MT0iMTIiIHkxPSIwIiB4Mj0iMTIiIHkyPSIyNCIgZ3JhZGllbnRVbml0cz0idXNlclNwYWNlT25Vc2UiPgo8c3RvcCBzdG9wLWNvbG9yPSIjQzE4M0ZGIi8%2BCjxzdG9wIG9mZnNldD0iMSIgc3RvcC1jb2xvcj0iIzREQzlGRiIvPgo8L2xpbmVhckdyYWRpZW50Pgo8L2RlZnM%2BCjwvc3ZnPgo%3D)](https://app.osaas.io/browse/eyevinn-player-analytics-worker)


Eyevinn Open Analytics is an open source solution for tracking events from video players. Based on the open standard Eyevinn Player Analytics ([EPAS](https://github.com/Eyevinn/player-analytics-specification/tree/main)) it enables a modular framework where you are not locked in with a specific vendor. This is the worker module that process the data from the eventsink and processing queue and stores it in a database.

## Hosted Solution

Available as an open web service in [Eyevinn Open Source Cloud](https://www.osaas.io). Read this [documentation to quickly get started](https://docs.osaas.io/osaas.wiki/Service%3A-Player-Analytics-Worker.html) with the hosted solution.

## Development

To run it with SQS and Clickhouse hosted in Eyevinn Open Source Cloud:

```
% npm install
% npm run build
% AWS_ACCESS_KEY_ID=<SQS-ACCESS-KEY-ID> \
  AWS_SECRET_ACCESS_KEY=<SQS-SECRET-ACCESS-KEY> \
  SQS_ENDPOINT=https://<tenant-id>-<instance-name>.poundifdef-smoothmq.auto.prod.osaas.io \
  SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/1/events \
  CLICKHOUSE_URL=https://<username>:<password>@<tenant-id>-<instance-name>.clickhouse-clickhouse.auto.prod.osaas.io \ 
  DB_TYPE=CLICKHOUSE \
  QUEUE_TYPE=SQS \
  AWS_REGION=dummy \
  npm start
```

The workers should start polling the SQS queue for messages and writing them to the Clickhouse database.

# Support

Join our [community on Slack](http://slack.osaas.io) where you can post any questions regarding any of our open source projects. Eyevinn's consulting business can also offer you:

- Further development of this component
- Customization and integration of this component into your platform
- Support and maintenance agreement

Contact [sales@eyevinn.se](mailto:sales@eyevinn.se) if you are interested.

# About Eyevinn Technology

Eyevinn Technology is an independent consultant firm specialized in video and streaming. Independent in a way that we are not commercially tied to any platform or technology vendor.

At Eyevinn, every software developer consultant has a dedicated budget reserved for open source development and contribution to the open source community. This give us room for innovation, team building and personal competence development. And also gives us as a company a way to contribute back to the open source community.

Want to know more about Eyevinn and how it is to work here. Contact us at work@eyevinn.se!
