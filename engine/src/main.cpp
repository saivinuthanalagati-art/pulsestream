#include "server.h"
#include <iostream>

int main() {
  try {
    PulseStreamServer server(9000);
    std::cout << "PulseStream Engine listening on port 9000 (TCP, NDJSON)\n";
    server.run();
  } catch (const std::exception& e) {
    std::cerr << "Fatal: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
