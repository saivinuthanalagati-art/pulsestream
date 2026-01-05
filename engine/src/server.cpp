#include "server.h"
#include "store.h"
#include "json.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <thread>

using json = nlohmann::json;

static std::string read_line(int fd) {
  std::string line;
  char ch;
  while (true) {
    ssize_t n = ::recv(fd, &ch, 1, 0);
    if (n == 0) return "";
    if (n < 0) { if (errno == EINTR) continue; throw std::runtime_error("recv failed"); }
    if (ch == '\n') break;
    line.push_back(ch);
    if (line.size() > 2 * 1024 * 1024) throw std::runtime_error("request too large");
  }
  return line;
}

static void write_line(int fd, const std::string& s) {
  std::string out = s + "\n";
  const char* p = out.c_str();
  size_t left = out.size();
  while (left > 0) {
    ssize_t n = ::send(fd, p, left, 0);
    if (n < 0) { if (errno == EINTR) continue; throw std::runtime_error("send failed"); }
    p += n; left -= (size_t)n;
  }
}

PulseStreamServer::PulseStreamServer(uint16_t port) : listen_fd_(-1), port_(port) {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) throw std::runtime_error("socket failed");

  int yes = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port_);

  if (::bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) throw std::runtime_error("bind failed");
  if (::listen(listen_fd_, 128) < 0) throw std::runtime_error("listen failed");
}

PulseStreamServer::~PulseStreamServer() {
  if (listen_fd_ >= 0) ::close(listen_fd_);
}

void PulseStreamServer::run() {
  while (true) {
    sockaddr_in client_addr{};
    socklen_t len = sizeof(client_addr);
    int client_fd = ::accept(listen_fd_, (sockaddr*)&client_addr, &len);
    if (client_fd < 0) { if (errno == EINTR) continue; throw std::runtime_error("accept failed"); }
    std::thread(&PulseStreamServer::handle_client, this, client_fd).detach();
  }
}

void PulseStreamServer::handle_client(int client_fd) {
  try {
    while (true) {
      std::string line = read_line(client_fd);
      if (line.empty()) break;

      json req;
      try { req = json::parse(line); }
      catch (...) { write_line(client_fd, json({{"ok", false},{"error","invalid_json"}}).dump()); continue; }

      const std::string type = req.value("type", "");

      if (type == "PING") {
        write_line(client_fd, json({{"ok", true},{"type","PONG"}}).dump());
        continue;
      }

      if (type == "CREATE_TOPIC") {
        std::string topic = req.value("topic","");
        int parts = req.value("partitions", 3);
        if (topic.empty()) { write_line(client_fd, json({{"ok",false},{"error","missing_topic"}}).dump()); continue; }
        bool ok = GlobalStore::instance().create_topic(topic, parts);
        write_line(client_fd, json({{"ok",ok},{"topic",topic},{"partitions",parts}}).dump());
        continue;
      }

      if (type == "TOPICS") {
        write_line(client_fd, json({{"ok",true},{"topics",GlobalStore::instance().list_topics()}}).dump());
        continue;
      }

      if (type == "PRODUCE") {
        std::string topic = req.value("topic","");
        std::string key = req.value("key","");
        std::string value = req.value("value","");
        if (topic.empty()) { write_line(client_fd, json({{"ok",false},{"error","missing_topic"}}).dump()); continue; }

        auto [partition, offset] = GlobalStore::instance().produce(topic, key, value);
        write_line(client_fd, json({{"ok",true},{"topic",topic},{"partition",partition},{"offset",offset}}).dump());
        continue;
      }

      if (type == "FETCH") {
        std::string topic = req.value("topic","");
        int partition = req.value("partition",0);
        long long offset_ll = req.value("offset",0LL);
        int limit = req.value("limit",10);

        if (topic.empty() || offset_ll < 0) { write_line(client_fd, json({{"ok",false},{"error","bad_request"}}).dump()); continue; }
        if (limit <= 0) limit = 10; if (limit > 1000) limit = 1000;

        auto batch = GlobalStore::instance().fetch(topic, partition, (uint64_t)offset_ll, limit);
        write_line(client_fd, json({{"ok",true},{"topic",topic},{"partition",partition},{"next_offset",batch.next_offset},{"records",batch.records}}).dump());
        continue;
      }

      if (type == "COMMIT") {
        std::string group = req.value("group","");
        std::string topic = req.value("topic","");
        int partition = req.value("partition",0);
        long long next_offset_ll = req.value("next_offset",0LL);

        if (group.empty() || topic.empty() || next_offset_ll < 0) { write_line(client_fd, json({{"ok",false},{"error","bad_request"}}).dump()); continue; }

        bool ok = GlobalStore::instance().commit_offset(group, topic, partition, (uint64_t)next_offset_ll);
        write_line(client_fd, json({{"ok",ok},{"group",group},{"topic",topic},{"partition",partition},{"committed_next_offset",(uint64_t)next_offset_ll}}).dump());
        continue;
      }

      if (type == "FETCH_GROUP") {
        std::string group = req.value("group","");
        std::string topic = req.value("topic","");
        int partition = req.value("partition",0);
        int limit = req.value("limit",10);
        bool auto_commit = req.value("auto_commit", true);

        if (group.empty() || topic.empty()) { write_line(client_fd, json({{"ok",false},{"error","bad_request"}}).dump()); continue; }
        if (limit <= 0) limit = 10; if (limit > 1000) limit = 1000;

        uint64_t start = GlobalStore::instance().get_committed_offset(group, topic, partition);
        auto batch = GlobalStore::instance().fetch(topic, partition, start, limit);

        bool commit_ok = true;
        uint64_t committed_after = start;
        if (auto_commit) {
          commit_ok = GlobalStore::instance().commit_offset(group, topic, partition, batch.next_offset);
          committed_after = batch.next_offset;
        }

        write_line(client_fd, json({
          {"ok",true},{"group",group},{"topic",topic},{"partition",partition},
          {"start_offset",start},{"next_offset",batch.next_offset},
          {"auto_commit",auto_commit},{"commit_ok",commit_ok},{"committed_offset_after",committed_after},
          {"records",batch.records}
        }).dump());
        continue;
      }

      if (type == "GROUP_STATS") {
        std::string group = req.value("group","");
        if (group.empty()) { write_line(client_fd, json({{"ok",false},{"error","missing_group"}}).dump()); continue; }
        write_line(client_fd, json({{"ok",true},{"stats",GlobalStore::instance().group_stats(group)}}).dump());
        continue;
      }

      write_line(client_fd, json({{"ok",false},{"error","unknown_type"},{"got",type}}).dump());
    }
  } catch (...) {}
  ::close(client_fd);
}
