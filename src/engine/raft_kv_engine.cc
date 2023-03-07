// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "engine/raft_kv_engine.h"

#include "butil/endpoint.h"
#include "common/helper.h"
#include "config/config_manager.h"
#include "proto/common.pb.h"
#include "raft/state_machine.h"
#include "server/server.h"

namespace dingodb {

RaftKvEngine::RaftKvEngine(std::shared_ptr<Engine> engine)
    : engine_(engine),
      raft_node_manager_(std::move(std::make_unique<RaftNodeManager>())) {}

RaftKvEngine::~RaftKvEngine() {}

bool RaftKvEngine::Init(std::shared_ptr<Config> config) {
  LOG(INFO) << "Now=> Int Raft Kv Engine with config[" << config->ToString();
  return true;
}

std::string RaftKvEngine::GetName() {
  return pb::common::Engine_Name(pb::common::ENG_RAFT_STORE);
}

pb::common::Engine RaftKvEngine::GetID() { return pb::common::ENG_RAFT_STORE; }

pb::error::Errno RaftKvEngine::AddRegion(
    std::shared_ptr<Context> ctx,
    const std::shared_ptr<pb::common::Region> region) {
  std::shared_ptr<RaftNode> node = std::make_shared<RaftNode>(
      region->id(), braft::PeerId(Server::GetInstance()->get_raft_endpoint()),
      new StoreStateMachine(engine_));

  if (node->Init(Helper::FormatPeers(
          Helper::ExtractLocations(region->peers()))) != 0) {
    node->Destroy();
    return pb::error::ERAFT_INIT;
  }

  raft_node_manager_->AddNode(region->id(), node);
  return pb::error::OK;
}

pb::error::Errno RaftKvEngine::ChangeRegion(
    std::shared_ptr<Context> ctx, uint64_t region_id,
    std::vector<pb::common::Peer> peers) {
  raft_node_manager_->GetNode(region_id)->ChangePeers(peers, nullptr);
  return pb::error::OK;
}

pb::error::Errno RaftKvEngine::DestroyRegion(std::shared_ptr<Context> ctx,
                                             uint64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return pb::error::ERAFT_NOTNODE;
  }
  node->Shutdown(nullptr);
  node->Join();

  raft_node_manager_->DeleteNode(region_id);
  return pb::error::OK;
}

pb::error::Errno RaftKvEngine::KvGet(std::shared_ptr<Context> ctx,
                                     const std::string& key,
                                     std::string& value) {
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }

  return engine_->KvGet(ctx, key, value);
}

pb::error::Errno RaftKvEngine::KvBatchGet(
    std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
    std::vector<pb::common::KeyValue>& kvs) {
  return engine_->KvBatchGet(ctx, keys, kvs);
}

std::shared_ptr<pb::raft::RaftCmdRequest> genRaftCmdRequest(
    const std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) {
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd =
      std::make_shared<pb::raft::RaftCmdRequest>();

  pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  header->set_region_id(ctx->region_id());

  auto request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::PUT);
  pb::raft::PutRequest* put_request = request->mutable_put();
  put_request->set_cf_name(ctx->cf_name());
  auto kv_req = put_request->add_kvs();
  *kv_req = kv;

  return raft_cmd;
}

pb::error::Errno RaftKvEngine::KvPut(std::shared_ptr<Context> ctx,
                                     const pb::common::KeyValue& kv) {
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }

  return node->Commit(ctx, genRaftCmdRequest(ctx, kv));
}

std::shared_ptr<pb::raft::RaftCmdRequest> genRaftCmdRequest(
    const std::shared_ptr<Context> ctx,
    const std::vector<pb::common::KeyValue>& kvs) {
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd =
      std::make_shared<pb::raft::RaftCmdRequest>();

  pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  header->set_region_id(ctx->region_id());

  auto request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::PUT);

  pb::raft::PutRequest* put_request = request->mutable_put();
  put_request->set_cf_name(ctx->cf_name());
  Helper::VectorToPbRepeated(kvs, put_request->mutable_kvs());

  return raft_cmd;
}

pb::error::Errno RaftKvEngine::KvBatchPut(
    std::shared_ptr<Context> ctx,
    const std::vector<pb::common::KeyValue>& kvs) {
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }

  return node->Commit(ctx, genRaftCmdRequest(ctx, kvs));
}

std::shared_ptr<pb::raft::RaftCmdRequest> genRaftCmdPutIfAbsentRequest(
    const std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) {
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd =
      std::make_shared<pb::raft::RaftCmdRequest>();

  pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  header->set_region_id(ctx->region_id());

  auto request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::PUTIFABSENT);
  pb::raft::PutIfAbsentRequest* put_if_absent_request =
      request->mutable_put_if_absent();
  put_if_absent_request->set_cf_name(ctx->cf_name());
  auto kv_req = put_if_absent_request->add_kvs();
  *kv_req = kv;

  return raft_cmd;
}

pb::error::Errno RaftKvEngine::KvPutIfAbsent(std::shared_ptr<Context> ctx,
                                             const pb::common::KeyValue& kv) {
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }

  return node->Commit(ctx, genRaftCmdPutIfAbsentRequest(ctx, kv));
}

std::shared_ptr<pb::raft::RaftCmdRequest> genRaftCmdPutIfAbsentRequest(
    const std::shared_ptr<Context> ctx,
    const std::vector<pb::common::KeyValue>& kvs) {
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd =
      std::make_shared<pb::raft::RaftCmdRequest>();

  pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  header->set_region_id(ctx->region_id());

  auto request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::PUTIFABSENT);

  pb::raft::PutIfAbsentRequest* put_if_absent_request =
      request->mutable_put_if_absent();
  put_if_absent_request->set_cf_name(ctx->cf_name());
  Helper::VectorToPbRepeated(kvs, put_if_absent_request->mutable_kvs());

  return raft_cmd;
}

pb::error::Errno RaftKvEngine::KvBatchPutIfAbsent(
    std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
    std::vector<std::string>& put_keys) {
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }

  return node->Commit(ctx, genRaftCmdPutIfAbsentRequest(ctx, kvs));
}

std::shared_ptr<pb::raft::RaftCmdRequest> genRaftCmdRequest(
    const std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd =
      std::make_shared<pb::raft::RaftCmdRequest>();

  pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  header->set_region_id(ctx->region_id());

  auto request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::PUT);
  pb::raft::DeleteRangeRequest* delete_range_request =
      request->mutable_delete_range();
  delete_range_request->set_cf_name(ctx->cf_name());
  auto range_req = delete_range_request->add_ranges();
  *range_req = range;

  return raft_cmd;
}

pb::error::Errno RaftKvEngine::KvDeleteRange(std::shared_ptr<Context> ctx,
                                             const pb::common::Range& range) {
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }

  return node->Commit(ctx, genRaftCmdRequest(ctx, range));
}

}  // namespace dingodb
