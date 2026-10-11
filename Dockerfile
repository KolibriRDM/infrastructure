FROM golang:1.27.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked CGO_ENABLED=0 GOMAXPROCS=2 go build -p 2 -trimpath -o /out/service ./cmd/balancer \
    && CGO_ENABLED=0 GOMAXPROCS=2 go build -p 2 -trimpath -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
COPY --from=build /out/healthcheck /healthcheck
EXPOSE 8084 9084
ENTRYPOINT ["/service"]
