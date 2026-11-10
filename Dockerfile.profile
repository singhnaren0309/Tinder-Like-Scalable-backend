FROM node:20-alpine
WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm install
COPY shared/ ./shared/
COPY services/profile-service/ ./services/profile-service/
EXPOSE 8081
CMD ["npx", "ts-node", "services/profile-service/src/index.ts"]

