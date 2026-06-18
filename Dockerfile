FROM node:22-alpine
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm install --prefer-offline --omit=dev

COPY src/ ./src/

ENTRYPOINT ["node", "src/index.js"]
CMD ["--once"]
