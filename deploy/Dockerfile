# Builds one Taproot Go program, picked by CMD (agent or processor).
#   docker build -f deploy/Dockerfile --build-arg CMD=agent .

FROM golang:1.26.7-alpine AS build
WORKDIR /src
# Dependencies first, so they stay cached when only code changes.
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG CMD
RUN test -n "$CMD" && CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/$CMD

# Just the static binary: no shell, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
