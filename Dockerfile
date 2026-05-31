FROM mcr.microsoft.com/playwright:v1.60.0-noble

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

ENV PORT=3000

CMD ["npm", "start"]
