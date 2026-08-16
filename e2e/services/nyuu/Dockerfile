FROM node:20-alpine
RUN apk add --no-cache python3 make g++ && \
    npm install -g nyuu@0.4.2 && \
    apk del python3 make g++
ENTRYPOINT ["nyuu"]
