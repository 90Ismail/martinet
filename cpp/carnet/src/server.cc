// carnet-server: exposes carnet::DB over gRPC (proto/carnet/v1/carnet.proto).

#include <grpcpp/grpcpp.h>
#ifdef CARNET_WITH_REFLECTION
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#endif
#include <grpcpp/health_check_service_interface.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "carnet/db.h"
#include "carnet/v1/carnet.grpc.pb.h"

namespace {

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

Status ToGrpc(const carnet::Status& s) {
  if (s.ok()) return Status::OK;
  switch (s.code()) {
    case carnet::Status::Code::kNotFound: return Status(StatusCode::NOT_FOUND, s.message());
    case carnet::Status::Code::kInvalidArgument: return Status(StatusCode::INVALID_ARGUMENT, s.message());
    case carnet::Status::Code::kBusy: return Status(StatusCode::UNAVAILABLE, s.message());
    case carnet::Status::Code::kCorruption: return Status(StatusCode::DATA_LOSS, s.message());
    default: return Status(StatusCode::INTERNAL, s.ToString());
  }
}

class CarnetService final : public carnet::v1::Carnet::Service {
 public:
  explicit CarnetService(carnet::DB* db) : db_(db) {}

  Status Put(ServerContext*, const carnet::v1::PutRequest* req, carnet::v1::PutResponse* resp) override {
    carnet::WriteBatch b;
    b.Put(req->key(), req->value());
    carnet::SequenceNumber seq = 0;
    auto s = db_->Write(b, req->sync(), &seq);
    resp->set_sequence(seq);
    return ToGrpc(s);
  }

  Status PutIfAbsent(ServerContext*, const carnet::v1::PutRequest* req,
                     carnet::v1::PutIfAbsentResponse* resp) override {
    bool written = false;
    std::string existing;
    auto s = db_->PutIfAbsent(req->key(), req->value(), req->sync(), &written, &existing);
    resp->set_written(written);
    if (!written) resp->set_existing(existing);
    return ToGrpc(s);
  }

  Status Get(ServerContext*, const carnet::v1::GetRequest* req, carnet::v1::GetResponse* resp) override {
    std::string v;
    auto s = db_->Get(req->key(), &v);
    if (s.IsNotFound()) {
      resp->set_found(false);
      return Status::OK;
    }
    resp->set_found(s.ok());
    resp->set_value(std::move(v));
    return ToGrpc(s);
  }

  Status Delete(ServerContext*, const carnet::v1::DeleteRequest* req, carnet::v1::DeleteResponse* resp) override {
    carnet::WriteBatch b;
    b.Delete(req->key());
    carnet::SequenceNumber seq = 0;
    auto s = db_->Write(b, req->sync(), &seq);
    resp->set_sequence(seq);
    return ToGrpc(s);
  }

  Status Batch(ServerContext*, const carnet::v1::BatchRequest* req, carnet::v1::BatchResponse* resp) override {
    carnet::WriteBatch b;
    for (const auto& op : req->ops()) {
      switch (op.kind()) {
        case carnet::v1::BatchOp::KIND_PUT: b.Put(op.key(), op.value()); break;
        case carnet::v1::BatchOp::KIND_DELETE: b.Delete(op.key()); break;
        default: return Status(StatusCode::INVALID_ARGUMENT, "batch op kind unspecified");
      }
    }
    carnet::SequenceNumber seq = 0;
    auto s = db_->Write(b, req->sync(), &seq);
    resp->set_sequence(seq);
    return ToGrpc(s);
  }

  Status Scan(ServerContext* ctx, const carnet::v1::ScanRequest* req,
              grpc::ServerWriter<carnet::v1::KeyValue>* writer) override {
    // Pin a snapshot so the stream is a consistent point-in-time view even
    // while writers keep going.
    const carnet::Snapshot* snap = db_->GetSnapshot();
    carnet::v1::KeyValue kv;
    auto s = db_->Scan(
        req->start(), req->end(), req->limit(),
        [&](std::string_view k, std::string_view v) {
          if (ctx->IsCancelled()) return false;
          kv.set_key(std::string(k));
          kv.set_value(std::string(v));
          return writer->Write(kv);
        },
        snap);
    db_->ReleaseSnapshot(snap);
    return ToGrpc(s);
  }

  Status Stats(ServerContext*, const carnet::v1::StatsRequest*, carnet::v1::StatsResponse* resp) override {
    auto st = db_->Stats();
    resp->set_last_sequence(st.last_sequence);
    resp->set_memtable_bytes(st.memtable_bytes);
    resp->set_compactions(st.compactions);
    resp->set_flushes(st.flushes);
    resp->set_bloom_negatives(st.bloom_negatives);
    for (size_t i = 0; i < st.levels.size(); i++) {
      auto* l = resp->add_levels();
      l->set_level(static_cast<uint32_t>(i));
      l->set_files(static_cast<uint32_t>(st.levels[i].first));
      l->set_bytes(st.levels[i].second);
    }
    return Status::OK;
  }

 private:
  carnet::DB* db_;
};

std::string Env(const char* name, const char* def) {
  const char* v = std::getenv(name);
  return v && *v ? v : def;
}

}  // namespace

int main(int argc, char** argv) {
  std::string dir = Env("CARNET_DIR", "/tmp/carnet");
  std::string addr = Env("CARNET_ADDR", "0.0.0.0:7070");
  for (int i = 1; i + 1 < argc; i += 2) {
    std::string flag = argv[i];
    if (flag == "--dir") dir = argv[i + 1];
    if (flag == "--addr") addr = argv[i + 1];
  }

  carnet::Options opts;
  opts.memtable_bytes = std::stoull(Env("CARNET_MEMTABLE_BYTES", "4194304"));
  std::unique_ptr<carnet::DB> db;
  auto s = carnet::DB::Open(opts, dir, &db);
  if (!s.ok()) {
    std::fprintf(stderr, "open %s: %s\n", dir.c_str(), s.ToString().c_str());
    return 1;
  }

  // Block termination signals in every thread; one thread waits for them and
  // shuts the server down so in-flight RPCs drain before the DB closes.
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  CarnetService svc(db.get());
  grpc::EnableDefaultHealthCheckService(true);  // k8s grpc liveness probes
#ifdef CARNET_WITH_REFLECTION
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();  // grpcurl works
#endif
  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&svc);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) {
    std::fprintf(stderr, "failed to listen on %s\n", addr.c_str());
    return 1;
  }
  std::fprintf(stderr, "carnet listening on %s, data in %s\n", addr.c_str(), dir.c_str());

  std::thread waiter([&] {
    int sig = 0;
    sigwait(&set, &sig);
    std::fprintf(stderr, "carnet: signal %d, draining\n", sig);
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
  });
  server->Wait();
  waiter.join();
  db.reset();  // joins the compaction thread
  return 0;
}
