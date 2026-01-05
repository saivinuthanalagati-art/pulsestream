FROM node:20-alpine
WORKDIR /app
COPY package.json /app/package.json
RUN npm install
COPY src /app/src
EXPOSE 8080
CMD ["node", "src/index.js"]
