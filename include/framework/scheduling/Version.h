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
  typedef MutableBuffer<RecordType> BufferType;
  typedef ExtensionStructure<ShardType, QueryType> StructureType;
  typedef BufferView<RecordType> BufferViewType;

public:

  Version(size_t vid = 0)
      : m_buffer(nullptr), m_structure(nullptr), m_id(vid), m_buffer_head(0) {}

  Version(size_t number, std::unique_ptr<StructureType> structure, BufferType *buff,
          size_t head)
      : m_buffer(buff), m_structure(std::move(structure)), m_id(number),
        m_buffer_head(head) {
          if (m_buffer) {
            m_buffer->take_head_reference(m_buffer_head);
          }
        }

  ~Version() {
    if (m_buffer) {
      m_buffer->release_head_reference(m_buffer_head);
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

  size_t get_id() const { return m_id; }

  void set_id(size_t id) { m_id = id;}

  const StructureType *get_structure() const { return m_structure.get(); }

  StructureType *get_mutable_structure() { return m_structure.get(); }

  bool set_structure(std::unique_ptr<StructureType> new_struct) {
    m_structure = std::move(new_struct);
    return true;
  }

  BufferViewType get_buffer() const {
    return m_buffer->get_buffer_view(m_buffer_head);
  }

  /*
   * Returns a new Version object that is a copy of this one. The new object
   * will also contain a copy of the m_structure, rather than a reference to
   * the same one. The epoch number of the new epoch will be set to the
   * provided argument.
   */
  Version *clone(size_t number) {
    auto version = new Version(number);
    version->m_buffer = m_buffer;
    version->m_buffer_head = m_buffer_head;
    if (version->m_buffer) {
      version->m_buffer->take_head_reference(m_buffer_head);
    }

    if (m_structure) {
      version->m_structure = std::unique_ptr(m_structure->copy());
    }

    return version;
  }

  bool advance_buffer_head(size_t new_head) {
    m_buffer->release_head_reference(m_buffer_head);
    if (m_buffer->advance_head(new_head)) {
      m_buffer_head = new_head;
      return true;
    }

    /* if we failed to advance, reclaim our reference */
    m_buffer->take_head_reference(m_buffer_head);
    return false;
  }

  void update_shard_version(size_t version) {
    m_structure->update_shard_version(version);
  }

  size_t get_head() {
    return m_buffer_head;
  }


  void set_buffer(BufferType *buffer, size_t head) {
    assert(m_buffer == nullptr);

    m_buffer = buffer;
    m_buffer_head = head;
    m_buffer->take_head_reference(head);
  }

  bool valid() {
    return (m_buffer) && (m_buffer_head) && (m_structure) && (m_id);
  }

private:
  BufferType *m_buffer;
  std::unique_ptr<StructureType> m_structure;

  size_t m_id;
  size_t m_buffer_head;
};
} // namespace de
