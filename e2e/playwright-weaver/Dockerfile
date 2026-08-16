FROM mcr.microsoft.com/playwright:v1.58.2-noble

WORKDIR /work
COPY package.json package-lock.json ./
RUN npm ci
COPY . .

ENV CI=1
CMD ["npm", "test"]
