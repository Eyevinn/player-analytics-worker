# player-analytics-worker

## Setup

To be able to build and run the project locally a things need to be set first.

- A `.npmrc` file is needed with the following content:

``` txt
@eyevinn:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=<token>
```

Where `<token>` is your personal GitHub access token, for more information see [link](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry#authenticating-with-a-personal-access-token).

- Environment Variables need to be set. You can use the `dotenv.template` file as a guide as to what needs to be set.

## Demo (SQS + DDB)

If you have set all the necessary environment variables, then you can demo the project.

run:

- `PUBLISH_PACKAGES=<token> npm install`
- `npm run build`
- `npm run dev`

...and the workers should start polling the SQS queue for messages and writing them to DynamoDB.
