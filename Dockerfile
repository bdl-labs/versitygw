FROM golang:1.25-alpine AS build

RUN apk add --no-cache ca-certificates git

# Set build arguments with default values
ARG VERSION="none"
ARG BUILD="none"
ARG TIME="none"

# Set environment variables
ENV VERSION=${VERSION}
ENV BUILD=${BUILD}
ENV TIME=${TIME}

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

WORKDIR /app/cmd/versitygw
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags "-s -w -X=main.Build=${BUILD} -X=main.BuildTime=${TIME} -X=main.Version=${VERSION}" -o versitygw

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

# These arguments can be overridden when building the image
ARG IAM_DIR=/tmp/vgw
ARG SETUP_DIR=/tmp/vgw

RUN mkdir -p $IAM_DIR
RUN mkdir -p $SETUP_DIR

COPY --from=0 /app/cmd/versitygw/versitygw /usr/local/bin/versitygw

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7070 7080 8080

ENTRYPOINT [ "/usr/local/bin/docker-entrypoint.sh" ]
