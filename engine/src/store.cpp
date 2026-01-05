#include "store.h"

#include <chrono>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;
using json = nlohmann::json;

static uint64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static uint64_t fnv1a_64(const std::string& s) {
  const uint64_t FNV_OFFSET = 1469598103934665603ULL;
  const uint64_t FNV_PRIME  = 1099511628211ULL;
  uint64_t h = FNV_OFFSET;
  for (unsigned char c : s) { h ^= (uint64_t)c; h *= FNV_PRIME; }
  return h;
}

// binary record: [u64 ts][u32 klen][u32 vlen][k][v]
static void write_u64(std::ofstream& out, uint64_t v){ out.write((const char*)&v, sizeof(v)); }
static void write_u32(std::ofstream& out, uint32_t v){ out.write((const char*)&v, sizeof(v)); }
static bool read_u64(std::ifstream& in, uint64_t& v){ in.read((char*)&v, sizeof(v)); return (bool)in; }
static bool read_u32(std::ifstream& in, uint32_t& v){ in.read((char*)&v, sizeof(v)); return (bool)in; }

static fs::path offsets_path(){ return fs::path("data") / "_offsets.json"; }

GlobalStore& GlobalStore::instance() {
  static GlobalStore s;
  return s;
}

GlobalStore::GlobalStore() {
  fs::create_directories("data");
}

void GlobalStore::load_offsets_locked() {
  if (offsets_loaded_) return;
  offsets_loaded_ = true;

  if (!fs::exists(offsets_path())) return;
  std::ifstream in(offsets_path());
  if (!in) return;

  json j;
  try { in >> j; } catch (...) { return; }
  if (!j.is_object()) return;

  committed_.clear();
  for (auto& [group, gv] : j.items()) {
    if (!gv.is_object()) continue;
    for (auto& [topic, tv] : gv.items()) {
      if (!tv.is_array()) continue;
      std::vector<uint64_t> vec;
      for (auto& x : tv) {
        if (!x.is_number_unsigned()) { vec.clear(); break; }
        vec.push_back(x.get<uint64_t>());
      }
      if (!vec.empty()) committed_[group][topic] = std::move(vec);
    }
  }
}

void GlobalStore::persist_offsets_locked() {
  fs::create_directories("data");
  json j = json::object();

  for (auto& [group, topics] : committed_) {
    json tg = json::object();
    for (auto& [topic, vec] : topics) {
      json arr = json::array();
      for (auto v : vec) arr.push_back(v);
      tg[topic] = arr;
    }
    j[group] = tg;
  }

  auto tmp = offsets_path(); tmp += ".tmp";
  { std::ofstream out(tmp); out << j.dump(2); }
  std::error_code ec;
  fs::rename(tmp, offsets_path(), ec);
  if (ec) { std::ofstream out(offsets_path()); out << j.dump(2); }
}

bool GlobalStore::create_topic(const std::string& topic, int partitions) {
  if (topic.empty() || partitions <= 0 || partitions > 128) return false;

  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();

  if (topics_.count(topic)) return false;

  TopicState st;
  st.partitions = partitions;

  fs::path dir = fs::path("data") / topic;
  fs::create_directories(dir);

  st.log_paths.resize(partitions);
  st.index_pos.resize(partitions);

  for (int p = 0; p < partitions; p++) {
    fs::path log = dir / ("p" + std::to_string(p) + ".log");
    st.log_paths[p] = log.string();
    std::ofstream out(st.log_paths[p], std::ios::binary | std::ios::app);
  }

  topics_[topic] = std::move(st);
  return true;
}

void GlobalStore::ensure_loaded_topic_locked(const std::string& topic) {
  if (topics_.count(topic)) return;

  // auto-create with 3 partitions
  TopicState st;
  st.partitions = 3;

  fs::path dir = fs::path("data") / topic;
  fs::create_directories(dir);

  st.log_paths.resize(st.partitions);
  st.index_pos.resize(st.partitions);

  for (int p = 0; p < st.partitions; p++) {
    fs::path log = dir / ("p" + std::to_string(p) + ".log");
    st.log_paths[p] = log.string();
    std::ofstream out(st.log_paths[p], std::ios::binary | std::ios::app);
  }

  // rebuild index by scanning disk
  for (int p = 0; p < st.partitions; p++) {
    std::ifstream in(st.log_paths[p], std::ios::binary);
    uint64_t pos = 0;
    while (true) {
      uint64_t ts; uint32_t klen, vlen;
      in.seekg((std::streamoff)pos, std::ios::beg);
      if (!read_u64(in, ts)) break;
      if (!read_u32(in, klen)) break;
      if (!read_u32(in, vlen)) break;

      if (klen > 10*1024*1024 || vlen > 50*1024*1024) break;
      in.seekg((std::streamoff)(klen + vlen), std::ios::cur);
      if (!in) break;

      st.index_pos[p].push_back(pos);
      pos = (uint64_t)in.tellg();
      if (pos == (uint64_t)-1) break;
    }
  }

  topics_[topic] = std::move(st);
}

std::pair<int, uint64_t> GlobalStore::produce(const std::string& topic,
                                              const std::string& key,
                                              const std::string& value) {
  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();
  ensure_loaded_topic_locked(topic);

  auto& st = topics_[topic];
  int partitions = st.partitions;

  int partition = 0;
  if (!key.empty()) partition = (int)(fnv1a_64(key) % (uint64_t)partitions);
  else partition = (int)((st.rr_counter++) % (uint64_t)partitions);

  uint64_t offset = (uint64_t)st.index_pos[partition].size();

  std::ofstream out(st.log_paths[partition], std::ios::binary | std::ios::app);
  if (!out) throw std::runtime_error("open log append failed");

  uint64_t file_pos = (uint64_t)out.tellp();
  st.index_pos[partition].push_back(file_pos);

  uint64_t ts = now_ms();
  uint32_t klen = (uint32_t)key.size();
  uint32_t vlen = (uint32_t)value.size();

  write_u64(out, ts);
  write_u32(out, klen);
  write_u32(out, vlen);
  out.write(key.data(), key.size());
  out.write(value.data(), value.size());
  out.flush();

  return {partition, offset};
}

FetchResult GlobalStore::fetch(const std::string& topic, int partition, uint64_t offset, int limit) {
  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();
  ensure_loaded_topic_locked(topic);

  FetchResult out;
  out.records = json::array();
  out.next_offset = offset;

  auto& st = topics_[topic];
  if (partition < 0 || partition >= st.partitions) return out;

  auto& idx = st.index_pos[partition];
  uint64_t end_offset = (uint64_t)idx.size();
  if (offset >= end_offset) { out.next_offset = end_offset; return out; }

  if (limit <= 0) limit = 10;
  if (limit > 1000) limit = 1000;

  std::ifstream in(st.log_paths[partition], std::ios::binary);
  if (!in) throw std::runtime_error("open log read failed");

  int count = 0;
  uint64_t i = offset;
  while (i < end_offset && count < limit) {
    uint64_t pos = idx[(size_t)i];
    in.seekg((std::streamoff)pos, std::ios::beg);

    uint64_t ts; uint32_t klen, vlen;
    if (!read_u64(in, ts)) break;
    if (!read_u32(in, klen)) break;
    if (!read_u32(in, vlen)) break;
    if (klen > 10*1024*1024 || vlen > 50*1024*1024) break;

    std::string k(klen, '\0'), v(vlen, '\0');
    if (klen) in.read(&k[0], klen);
    if (vlen) in.read(&v[0], vlen);
    if (!in) break;

    out.records.push_back({
      {"partition", partition},
      {"offset", i},
      {"ts_ms", ts},
      {"key", k},
      {"value", v}
    });

    i++; count++;
  }

  out.next_offset = i;
  return out;
}

bool GlobalStore::commit_offset(const std::string& group, const std::string& topic, int partition, uint64_t next_offset) {
  if (group.empty() || topic.empty()) return false;

  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();
  ensure_loaded_topic_locked(topic);

  auto& st = topics_[topic];
  if (partition < 0 || partition >= st.partitions) return false;

  auto& vec = committed_[group][topic];
  if ((int)vec.size() < st.partitions) vec.resize(st.partitions, 0);

  uint64_t end_offset = (uint64_t)st.index_pos[partition].size();
  if (next_offset > end_offset) next_offset = end_offset;

  vec[partition] = next_offset;
  persist_offsets_locked();
  return true;
}

uint64_t GlobalStore::get_committed_offset(const std::string& group, const std::string& topic, int partition) {
  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();
  ensure_loaded_topic_locked(topic);

  auto itg = committed_.find(group);
  if (itg == committed_.end()) return 0;
  auto itt = itg->second.find(topic);
  if (itt == itg->second.end()) return 0;
  auto& vec = itt->second;
  if (partition < 0 || partition >= (int)vec.size()) return 0;
  return vec[partition];
}

json GlobalStore::list_topics() {
  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();

  json arr = json::array();
  for (auto& [name, st] : topics_) {
    json parts = json::array();
    for (int p = 0; p < st.partitions; p++) {
      parts.push_back({
        {"partition", p},
        {"end_offset", (uint64_t)st.index_pos[p].size()}
      });
    }
    arr.push_back({{"topic", name}, {"partitions", st.partitions}, {"partition_stats", parts}});
  }
  return arr;
}

json GlobalStore::group_stats(const std::string& group) {
  std::lock_guard<std::mutex> lock(mu_);
  load_offsets_locked();

  json topics = json::array();
  for (auto& [topic, st] : topics_) {
    json parts = json::array();
    for (int p = 0; p < st.partitions; p++) {
      uint64_t end_offset = (uint64_t)st.index_pos[p].size();
      uint64_t committed = 0;

      auto itg = committed_.find(group);
      if (itg != committed_.end()) {
        auto itt = itg->second.find(topic);
        if (itt != itg->second.end() && p < (int)itt->second.size()) committed = itt->second[p];
      }

      if (committed > end_offset) committed = end_offset;

      parts.push_back({
        {"partition", p},
        {"end_offset", end_offset},
        {"committed_offset", committed},
        {"lag", end_offset - committed}
      });
    }

    topics.push_back({
      {"topic", topic},
      {"partitions", st.partitions},
      {"partitions_stats", parts}
    });
  }

  return json({{"group", group}, {"topics", topics}});
}
