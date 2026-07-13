FROM mcr.microsoft.com/playwright:v1.60.0-noble

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund

COPY server.js ./

ENV NODE_ENV=production

CMD ["npm", "start"]

