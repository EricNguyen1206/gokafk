# Stage 1: build
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /gokafk ./cmd/gokafk

# Stage 2: minimal runtime
FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /gokafk /gokafk
EXPOSE 10000
ENTRYPOINT ["/gokafk", "server"]
