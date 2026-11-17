// Package platform holds the boring-but-necessary plumbing every Martinet
// binary shares: env config, structured logging, Prometheus metrics, gRPC
// servers with health checks, and signal-driven shutdown.
package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Env returns the environment variable or a default.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt parses an integer environment variable.
func EnvInt(key string, def int) int {
	v, err := strconv.Atoi(Env(key, strconv.Itoa(def)))
	if err != nil {
		return def
	}
	return v
}

// EnvDuration parses a Go duration ("10s", "2m").
func EnvDuration(key string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(Env(key, def.String()))
	if err != nil {
		return def
	}
	return d
}

// Logger returns a JSON logger tagged with the service name.
func Logger(service string) *slog.Logger {
	level := slog.LevelInfo
	if Env("LOG_LEVEL", "info") == "debug" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("svc", service)
}

// SignalContext is cancelled on SIGINT/SIGTERM (Kubernetes pod termination).
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ServeMetrics exposes /metrics, /healthz and any extra handlers on addr.
func ServeMetrics(ctx context.Context, log *slog.Logger, addr string, extra map[string]http.Handler) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	for p, h := range extra {
		mux.Handle(p, h)
	}
	ServeHTTP(ctx, log, addr, mux)
}

// ServeHTTP runs an HTTP server until ctx is cancelled, then drains it.
func ServeHTTP(ctx context.Context, log *slog.Logger, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go func() {
		log.Info("http listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
		}
	}()
}

// GRPCServer builds a server with the standard health service (used by
// Kubernetes gRPC probes) and reflection (so grpcurl works).
func GRPCServer(opts ...grpc.ServerOption) (*grpc.Server, *health.Server) {
	s := grpc.NewServer(opts...)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(s, hs)
	reflection.Register(s)
	return s, hs
}

// ServeGRPC listens on addr and stops gracefully when ctx is cancelled.
func ServeGRPC(ctx context.Context, log *slog.Logger, addr string, s *grpc.Server, hs *health.Server) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		hs.Shutdown() // flip to NOT_SERVING so load balancers drain us first
		done := make(chan struct{})
		go func() { s.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			s.Stop()
		}
	}()
	go func() {
		log.Info("grpc listening", "addr", addr)
		if err := s.Serve(lis); err != nil {
			log.Error("grpc server", "err", err)
		}
	}()
	return nil
}

// Dial opens a plaintext gRPC client connection (in-cluster traffic; mTLS is
// the service mesh's job in production).
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
