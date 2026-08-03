# Nyuu is used only to create normal Usenet yEnc articles and NZBs. Pinning its
# version prevents posting implementation drift from affecting later runs.
FROM node:20.19.0-alpine3.21@sha256:8bda036ddd59ea51a23bc1a1035d3b5c614e72c01366d989f4120e8adca196d4

WORKDIR /app
COPY package.json package-lock.json ./

RUN set -eux; \
    apk add --no-cache --virtual .build-deps python3 make g++; \
    npm ci --omit=dev; \
    ./node_modules/.bin/nyuu --version; \
    apk del .build-deps

ENTRYPOINT ["/app/node_modules/.bin/nyuu"]
