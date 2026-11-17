#include "pouls/health_server.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "martinet/v1/martinet.grpc.pb.h"

namespace pouls {

martinet::v1::EndpointHealth ToProto(const Health& h, uint64_t late_events) {
  martinet::v1::EndpointHealth p;
  p.set_endpoint(h.endpoint);
  switch (h.state) {
    case State::kClosed: p.set_state(martinet::v1::BREAKER_STATE_CLOSED); break;
    case State::kOpen: p.set_state(martinet::v1::BREAKER_STATE_OPEN); break;
    case State::kHalfOpen: p.set_state(martinet::v1::BREAKER_STATE_HALF_OPEN); break;
  }
  p.set_failure_rate(h.failure_rate);
  p.set_attempts(h.attempts);
  p.set_failures(h.failures);
  p.set_window_start_ms(h.window_start_ms);
  p.set_window_end_ms(h.window_end_ms);
  p.set_open_until_ms(h.open_until_ms);
  p.set_late_events(late_events);
  p.set_p99_latency_ms(h.p99_latency_ms);
  return p;
}

class HealthServer::Impl final : public martinet::v1::Pulse::Service {
 public:
  Impl(const BreakerTable* t, const std::atomic<int64_t>* wm, const std::atomic<uint64_t>* late)
      : table_(t), watermark_(wm), late_(late) {}

  grpc::Status GetEndpointHealth(grpc::ServerContext*, const martinet::v1::GetEndpointHealthRequest* req,
                                 martinet::v1::EndpointHealth* resp) override {
    Health h;
    if (!table_->Get(req->endpoint(), &h)) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "no windows seen for " + req->endpoint());
    }
    *resp = ToProto(h, late_->load());
    return grpc::Status::OK;
  }

  grpc::Status ListEndpointHealth(grpc::ServerContext*, const martinet::v1::ListEndpointHealthRequest*,
                                  martinet::v1::ListEndpointHealthResponse* resp) override {
    for (const auto& h : table_->All()) *resp->add_endpoints() = ToProto(h, late_->load());
    resp->set_watermark_ms(watermark_->load());
    return grpc::Status::OK;
  }

 private:
  const BreakerTable* table_;
  const std::atomic<int64_t>* watermark_;
  const std::atomic<uint64_t>* late_;
};

HealthServer::HealthServer(const BreakerTable* table, const std::atomic<int64_t>* watermark,
                           const std::atomic<uint64_t>* late_events)
    : impl_(std::make_unique<Impl>(table, watermark, late_events)) {}

HealthServer::~HealthServer() { Shutdown(); }

bool HealthServer::Start(const std::string& addr) {
  grpc::EnableDefaultHealthCheckService(true);
  grpc::ServerBuilder b;
  b.AddListeningPort(addr, grpc::InsecureServerCredentials());
  b.RegisterService(impl_.get());
  server_ = b.BuildAndStart();
  return server_ != nullptr;
}

void HealthServer::Shutdown() {
  if (server_) {
    server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    server_.reset();
  }
}

}  // namespace pouls
