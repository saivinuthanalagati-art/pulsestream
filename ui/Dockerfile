FROM node:20-alpine
WORKDIR /app
COPY package.json /app/package.json
COPY vite.config.js /app/vite.config.js
COPY index.html /app/index.html
COPY src /app/src
RUN npm install
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
