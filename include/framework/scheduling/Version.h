/*
 * include/framework/scheduling/Version.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <mutex>

#include "framework/structure/BufferView.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/structure/MutableBuffer.h"

namespace de {

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class Version {
private:
  typedef typename ShardType::RECORD RecordType;
  typedef MutableBuffer<RecordType> Buffer;
  typedef ExtensionStructure<ShardType, QueryType> Structure;
  typedef BufferView<RecordType> BufView;

public:
  Version(size_t number = 0)
      : m_buffer(nullptr), m_structure(nullptr), m_active_merge(false),
        m_epoch_number(number), m_buffer_head(0) {}

  Version(size_t number, Structure *structure, Buffer *buff, size_t head)
      : m_buffer(buff), m_structure(structure), m_active_merge(false),
        m_epoch_number(number), m_buffer_head(head) {
    structure->take_reference();
  }

  ~Version() {
    if (m_structure) {
      m_structure->release_reference();
    }

    if (m_structure->get_reference_count() == 0) {
      delete m_structure;
    }
  }

  /*
   * Versions are *not* copyable or movable. Only one can exist, and all users
   * of it work with pointers
   */
  Version(const Version &) = delete;
  Version(Version &&) = delete;
  Version &operator=(const Version &) = delete;
  Version &operator=(Version &&) = delete;

  size_t get_epoch_number() const { return m_epoch_number; }

  const Structure *get_structure() const { return m_structure; }

  Structure *get_mutable_structure() { return m_structure; }

  BufView get_buffer() const { return m_buffer->get_buffer_view(m_buffer_head); }

  /*
   * Returns a new Version object that is a copy of this one. The new object
   * will also contain a copy of the m_structure, rather than a reference to
   * the same one. The epoch number of the new epoch will be set to the
   * provided argument.
   */
  Version *clone(size_t number) {
    std::unique_lock<std::mutex> m_buffer_lock;
    auto epoch = new Version(number);
    epoch->m_buffer = m_buffer;
    epoch->m_buffer_head = m_buffer_head;

    if (m_structure) {
      epoch->m_structure = m_structure->copy();
      /* the copy routine returns a structure with 0 references */
      epoch->m_structure->take_reference();
    }

    return epoch;
  }

  /*
   * Check if a merge can be started from this Version. At present, without
   * concurrent merging, this simply checks if there is currently a scheduled
   * merge based on this Version. If there is, returns false. If there isn't,
   * return true and set a flag indicating that there is an active merge.
   */
  bool prepare_reconstruction() {
    auto old = m_active_merge.load();
    if (old) {
      return false;
    }

    // FIXME: this needs cleaned up
    while (!m_active_merge.compare_exchange_strong(old, true)) {
      old = m_active_merge.load();
      if (old) {
        return false;
      }
    }

    return true;
  }

  bool advance_buffer_head(size_t head) {
    m_buffer_head = head;
    return m_buffer->advance_head(m_buffer_head);
  }

private:
  Buffer *m_buffer;
  Structure *m_structure;

  std::mutex m_buffer_lock;
  std::atomic<bool> m_active_merge;

  /*
   * The number of currently active jobs
   * (queries/merges) operating on this
   * epoch. An epoch can only be retired
   * when this number is 0.
   */
  size_t m_epoch_number;
  size_t m_buffer_head;
};
} // namespace de
