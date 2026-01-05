#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "json.hpp"

struct FetchResult {
  nlohmann::json records;   // JSON array
  uint64_t next_offset;
};

class GlobalStore {
public:
  static GlobalStore& instance();

  bool create_topic(const std::string& topic, int partitions);

  // Returns {partition, offset}
  std::pair<int, uint64_t> produce(const std::string& topic,
                                  const std::string& key,
                                  const std::string& value);

  FetchResult fetch(const std::string& topic, int partition, uint64_t offset, int limit);

  // Consumer group offsets
  bool commit_offset(const std::string& group, const std::string& topic, int partition, uint64_t next_offset);
  uint64_t get_committed_offset(const std::string& group, const std::string& topic, int partition);

  // Stats
  nlohmann::json list_topics();
  nlohmann::json group_stats(const std::string& group);

private:
  GlobalStore();
  void ensure_loaded_topic_locked(const std::string& topic);
  void load_offsets_locked();
  void persist_offsets_locked();

  struct TopicState {
    int partitions = 3;
    uint64_t rr_counter = 0;
    std::vector<std::string> log_paths;
    std::vector<std::vector<uint64_t>> index_pos; // byte positions for each record
  };

  std::mutex mu_;
  std::unordered_map<std::string, TopicState> topics_;

  // group -> topic -> vector(committed next_offset per partition)
  std::unordered_map<std::string, std::unordered_map<std::string, std::vector<uint64_t>>> committed_;
  bool offsets_loaded_ = false;
};
