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
      : m_buffer(nullptr), m_structure(nullptr), m_id(vid), m_buffer_head(0),
        m_pending_buffer_head(-1) {}

  Version(size_t number, std::unique_ptr<StructureType> structure, BufferType *buff,
          size_t head)
      : m_buffer(buff), m_structure(std::move(structure)), m_id(number),
        m_buffer_head(head), m_pending_buffer_head(-1) {}

  ~Version() = default;

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
    if (m_structure) {
      return false;
    }

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

    if (m_structure) {
      version->m_structure = std::unique_ptr(m_structure->copy());
    }

    return version;
  }

  bool advance_buffer_head(size_t new_head) {
    m_buffer_head = new_head;
    return m_buffer->advance_head(new_head);
  }

  void merge_changes_from(Version *old, size_t version_id) {
    /*
     * for a maint reconstruction, the buffer head may have advanced
     * during the reconstruction; we don't need to adjust the buffer
     * for maintenance reconstructions, so we can simply "catch" the
     * internal head index up to the current version.
     */
    if (old->m_buffer_head > m_buffer_head) {
      m_buffer_head = old->m_buffer_head;
    }

    // FIXME: we should also ensure that we don't clobber anything
    // in the event that multiple concurrent reconstructions affect
    // the same levels. As it stands, two reconstructions *could* share
    // source shards, resulting in some records being lost or duplicated.
    // 
    // For the moment, I'm being careful to avoid this within the
    // scheduling policy itself, and only forwarding changes to this
    // version.

    /* using INVALID_VERSION disables shard reconcilliation */
    if (version_id == 0) {
      return;
    }

    /* add any shards newer than version_id to this version */
    auto old_structure = old->get_structure();
    m_structure->merge_structure(old_structure, version_id);
  }

  void update_shard_version(size_t version) {
    m_structure->update_shard_version(version);
  }

private:
  BufferType *m_buffer;
  std::unique_ptr<StructureType> m_structure;

  size_t m_id;
  size_t m_buffer_head;
  ssize_t m_pending_buffer_head;
};
} // namespace de
