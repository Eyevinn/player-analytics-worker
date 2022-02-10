# player-analytics-worker

## Setup

To be able to build and run the project locally a few things need to be set first.

- Environment Variables need to be set. You can use the `dotenv.template` file as a guide as to what needs to be set.

## Demo (SQS + DDB)

If you have set all the necessary environment variables, then you can demo the project.

run:

- `npm run build`
- `npm run dev`

...and the workers should start polling the SQS queue for messages and writing them to DynamoDB.
