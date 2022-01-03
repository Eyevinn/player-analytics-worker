FROM node:slim

WORKDIR /app

ADD . .

ARG NPM_GITHUB_TOKEN
# This is needed for installing private github packages
RUN echo //npm.pkg.github.com/:_authToken=$NPM_GITHUB_TOKEN >> ~/.npmrc
RUN echo @eyevinn:registry=https://npm.pkg.github.com/ >> ~/.npmrc
RUN npm install
# Removing the github token from npmrc file in below line
RUN echo > ~/.npmrc

RUN npm run build

CMD ["npm", "start"]
