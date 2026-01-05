#include "json.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>

using json = nlohmann::json;

static void die(const std::string& msg) {
  std::cerr << "Error: " << msg << "\n";
  std::exit(1);
}

static int connect_to(const std::string& host, int port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) die("socket() failed");

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) die("inet_pton failed");
  if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) die(std::string("connect failed: ") + std::strerror(errno));
  return fd;
}

static void send_line(int fd, const std::string& s) {
  std::string out = s + "\n";
  const char* p = out.c_str();
  size_t left = out.size();
  while (left > 0) {
    ssize_t n = ::send(fd, p, left, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      die("send failed");
    }
    p += n;
    left -= (size_t)n;
  }
}

static std::string recv_line(int fd) {
  std::string line;
  char ch;
  while (true) {
    ssize_t n = ::recv(fd, &ch, 1, 0);
    if (n == 0) break;
    if (n < 0) {
      if (errno == EINTR) continue;
      die("recv failed");
    }
    if (ch == '\n') break;
    line.push_back(ch);
  }
  return line;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr <<
      "Usage:\n"
      "  client ping\n"
      "  client create-topic <topic> <partitions>\n"
      "  client topics\n"
      "  client produce <topic> <key> <value>\n"
      "  client fetch <topic> <partition> <offset> <limit>\n"
      "  client commit <group> <topic> <partition> <next_offset>\n"
      "  client group-stats <group>\n"
      "  client fetch-group <group> <topic> <partition> <limit> [--no-commit]\n";
    return 1;
  }

  std::string cmd = argv[1];
  json req;

  if (cmd == "ping") req = {{"type","PING"}};

  else if (cmd == "create-topic") {
    if (argc < 4) die("create-topic needs <topic> <partitions>");
    req = {{"type","CREATE_TOPIC"},{"topic",argv[2]},{"partitions",std::stoi(argv[3])}};
  }

  else if (cmd == "topics") req = {{"type","TOPICS"}};

  else if (cmd == "produce") {
    if (argc < 5) die("produce needs <topic> <key> <value>");
    req = {{"type","PRODUCE"},{"topic",argv[2]},{"key",argv[3]},{"value",argv[4]}};
  }

  else if (cmd == "fetch") {
    if (argc < 6) die("fetch needs <topic> <partition> <offset> <limit>");
    req = {{"type","FETCH"},{"topic",argv[2]},{"partition",std::stoi(argv[3])},
           {"offset",std::stoll(argv[4])},{"limit",std::stoi(argv[5])}};
  }

  else if (cmd == "commit") {
    if (argc < 6) die("commit needs <group> <topic> <partition> <next_offset>");
    req = {{"type","COMMIT"},{"group",argv[2]},{"topic",argv[3]},
           {"partition",std::stoi(argv[4])},{"next_offset",std::stoll(argv[5])}};
  }

  else if (cmd == "group-stats") {
    if (argc < 3) die("group-stats needs <group>");
    req = {{"type","GROUP_STATS"},{"group",argv[2]}};
  }

  // ✅ NEW: fetch-group
  else if (cmd == "fetch-group") {
    if (argc < 6) die("fetch-group needs <group> <topic> <partition> <limit> [--no-commit]");
    bool auto_commit = true;
    if (argc >= 7 && std::string(argv[6]) == "--no-commit") auto_commit = false;

    req = {{"type","FETCH_GROUP"},
           {"group",argv[2]},
           {"topic",argv[3]},
           {"partition",std::stoi(argv[4])},
           {"limit",std::stoi(argv[5])},
           {"auto_commit", auto_commit}};
  }

  else {
    die("unknown command");
  }

  int fd = connect_to("127.0.0.1", 9000);
  send_line(fd, req.dump());
  std::string res = recv_line(fd);
  ::close(fd);

  if (res.empty()) die("no response (engine running?)");
  try { std::cout << json::parse(res).dump(2) << "\n"; }
  catch (...) { std::cout << res << "\n"; }
  return 0;
}
