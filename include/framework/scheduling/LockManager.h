/*
 *
 */

#pragma once
#include <deque>
#include <atomic>

namespace de {
class LockManager {
public:
  LockManager(size_t levels=1) {
    for (size_t i=0; i < levels; i++) {
      m_lks.emplace_back(false);
    }
  }

  ~LockManager() = default;

  void add_lock() {
    m_lks.emplace_back(false);
  }

  void release_lock(size_t idx) {
    if (idx < m_lks.size()) {
      m_lks[idx].store(false);
    }
  }

  bool is_locked(size_t idx) {
    if (idx < m_lks.size()) {
      return m_lks[idx].load();
    }

    return false;
  }

  bool take_lock(size_t idx) {
    if (idx < m_lks.size()) {
      bool old = m_lks[idx].load();
      if (!old) {
        return m_lks[idx].compare_exchange_strong(old, true);
      }
    }

    return false;
  }

private:
  std::deque<std::atomic<bool>> m_lks;
};
}
