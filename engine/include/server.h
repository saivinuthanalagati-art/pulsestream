#pragma once
#include <cstdint>

class PulseStreamServer {
public:
  explicit PulseStreamServer(uint16_t port);
  ~PulseStreamServer();
  void run(); // blocking

private:
  int listen_fd_;
  uint16_t port_;
  void handle_client(int client_fd);
};
