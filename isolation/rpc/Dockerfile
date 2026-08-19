FROM golang:1.25.12-alpine3.23 AS build
WORKDIR /app
COPY ./process.go .
RUN go build process.go

FROM alpine:3.22
WORKDIR /app
COPY --from=build /app/process .
ENTRYPOINT [ "./process" ]
