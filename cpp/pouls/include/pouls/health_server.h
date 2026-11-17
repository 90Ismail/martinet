#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "martinet/v1/martinet.pb.h"
#include "pouls/breaker.h"

namespace grpc {
class Server;
}

namespace pouls {

martinet::v1::EndpointHealth ToProto(const Health& h, uint64_t late_events);

// Serves martinet.v1.Pulse so operators (martinetctl health) can read the
// live breaker table without going through Kafka.
class HealthServer {
 public:
  HealthServer(const BreakerTable* table, const std::atomic<int64_t>* watermark,
               const std::atomic<uint64_t>* late_events);
  ~HealthServer();
  bool Start(const std::string& addr);
  void Shutdown();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace pouls
