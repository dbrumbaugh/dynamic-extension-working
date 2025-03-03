/*
 *
 */

#pragma once
#include <deque>
#include <atomic>
#include <cassert>

namespace de {
class LockManager {
public:
  LockManager(size_t levels=1) {
    for (size_t i=0; i < levels; i++) {
      m_lks.emplace_back(false);
    }

    m_last_unlocked_version = 0;
  }

  ~LockManager() = default;

  void add_lock() {
    m_lks.emplace_back(false);
  }

  void release_lock(size_t idx, size_t version) {
    if (idx < m_lks.size()) {
      assert(m_lks.at(idx).load() == true);

      while (m_last_unlocked_version.load() < version) {
        auto tmp = m_last_unlocked_version.load();
        m_last_unlocked_version.compare_exchange_strong(tmp, version);
      }

      m_lks.at(idx).store(false);
    }
  }

  bool is_locked(size_t idx, size_t version) {
    if (idx < m_lks.size()) {
      return m_lks.at(idx).load() && m_last_unlocked_version <= version;
    }

    return false;
  }

  bool take_lock(size_t idx, size_t version) {
    if (idx < m_lks.size()) {
      bool old = m_lks.at(idx).load();
      if (!old) {
        auto result = m_lks.at(idx).compare_exchange_strong(old, true);

        if (m_last_unlocked_version.load() > version) {
          m_lks.at(idx).store(false);
          return false;
        }

        return result;
      }
    }

    return false;
  }

  bool take_buffer_lock() {
    bool old = m_buffer_lk.load();
    if (!old) {
      return m_buffer_lk.compare_exchange_strong(old, true);
    }

    return false;
  }

  bool is_buffer_locked() {
    return m_buffer_lk.load();
  }

  void release_buffer_lock() {
    m_buffer_lk.store(false);
  }

private:
  std::deque<std::atomic<bool>> m_lks;
  std::atomic<bool> m_buffer_lk;
  std::atomic<size_t> m_last_unlocked_version;
};
}
