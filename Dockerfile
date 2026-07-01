FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /zensu-monitoring-agent ./cmd/zensu-monitoring-agent

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /zensu-monitoring-agent /zensu-monitoring-agent
# Numeric uid:gid (distroless "nonroot" = 65532) so Kubernetes runAsNonRoot can
# verify the user without resolving a username.
USER 65532:65532
ENTRYPOINT ["/zensu-monitoring-agent"]
