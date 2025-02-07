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

#include "br/restore_sql_data.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

#ifndef ENABLE_RESTORE_SQL_DATA_PTHREAD
#define ENABLE_RESTORE_SQL_DATA_PTHREAD
#endif

// #undef ENABLE_RESTORE_SQL_DATA_PTHREAD

RestoreSqlData::RestoreSqlData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                               ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                               const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                               const std::string& storage_internal,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sql_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sql_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sql_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sql_data_sst)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      store_region_sql_data_sst_(store_region_sql_data_sst),
      store_cf_sst_meta_sql_data_sst_(store_cf_sst_meta_sql_data_sst),
      index_region_sql_data_sst_(index_region_sql_data_sst),
      index_cf_sst_meta_sql_data_sst_(index_cf_sst_meta_sql_data_sst),
      document_region_sql_data_sst_(document_region_sql_data_sst),
      document_cf_sst_meta_sql_data_sst_(document_cf_sst_meta_sql_data_sst) {}

RestoreSqlData::~RestoreSqlData() = default;

std::shared_ptr<RestoreSqlData> RestoreSqlData::GetSelf() { return shared_from_this(); }

butil::Status RestoreSqlData::Init() {
  butil::Status status;

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_region_sql_data_sst_->DebugString();
  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_region_sql_data_sst_->DebugString();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << index_region_sql_data_sst_->DebugString();
  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << index_cf_sst_meta_sql_data_sst_->DebugString();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << document_region_sql_data_sst_->DebugString();
  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << document_cf_sst_meta_sql_data_sst_->DebugString();

  status = CheckStoreRegionSqlDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckStoreCfSstMetaSqlDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckIndexRegionSqlDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckIndexCfSstMetaSqlDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckDocumentRegionSqlDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckDocumentCfSstMetaSqlDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::Run() { return butil::Status::OK(); }

butil::Status RestoreSqlData::Finish() { return butil::Status::OK(); }

butil::Status RestoreSqlData::CheckStoreRegionSqlDataSst() {
  butil::Status status;

  status = CheckRegionSqlDataSst(store_region_sql_data_sst_, dingodb::Constant::kStoreRegionSqlDataSstName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}
butil::Status RestoreSqlData::CheckStoreCfSstMetaSqlDataSst() {
  butil::Status status;

  status = CheckCfSstMetaSqlDataSst(store_cf_sst_meta_sql_data_sst_, dingodb::Constant::kStoreCfSstMetaSqlDataSstName,
                                    dingodb::Constant::kStoreRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::CheckIndexRegionSqlDataSst() {
  butil::Status status;

  status = CheckRegionSqlDataSst(index_region_sql_data_sst_, dingodb::Constant::kIndexRegionSqlDataSstName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::CheckIndexCfSstMetaSqlDataSst() {
  butil::Status status;

  status = CheckCfSstMetaSqlDataSst(index_cf_sst_meta_sql_data_sst_, dingodb::Constant::kIndexCfSstMetaSqlDataSstName,
                                    dingodb::Constant::kIndexRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::CheckDocumentRegionSqlDataSst() {
  butil::Status status;

  status = CheckRegionSqlDataSst(document_region_sql_data_sst_, dingodb::Constant::kDocumentRegionSqlDataSstName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::CheckDocumentCfSstMetaSqlDataSst() {
  butil::Status status;

  status =
      CheckCfSstMetaSqlDataSst(document_cf_sst_meta_sql_data_sst_, dingodb::Constant::kDocumentCfSstMetaSqlDataSstName,
                               dingodb::Constant::kDocumentRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::CheckRegionSqlDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> region_sql_data_sst, const std::string& file_name) {
  butil::Status status;

  status = Utils::CheckBackupMeta(region_sql_data_sst, storage_internal_, file_name, "",
                                  dingodb::Constant::kCoordinatorRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlData::CheckCfSstMetaSqlDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> cf_sst_meta_sql_data_sst, const std::string& file_name,
    const std::string& exec_node) {
  butil::Status status;

  status = Utils::CheckBackupMeta(cf_sst_meta_sql_data_sst, storage_internal_, file_name, "", exec_node);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

}  // namespace br