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

#include "br/restore_sdk_data.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>

#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace br {

#ifndef ENABLE_RESTORE_SDK_DATA_PTHREAD
#define ENABLE_RESTORE_SDK_DATA_PTHREAD
#endif

// #undef ENABLE_RESTORE_SDK_DATA_PTHREAD

RestoreSdkData::RestoreSdkData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                               ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                               const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                               const std::string& storage_internal,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sdk_data_sst)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      store_region_sdk_data_sst_(store_region_sdk_data_sst),
      store_cf_sst_meta_sdk_data_sst_(store_cf_sst_meta_sdk_data_sst),
      index_region_sdk_data_sst_(index_region_sdk_data_sst),
      index_cf_sst_meta_sdk_data_sst_(index_cf_sst_meta_sdk_data_sst),
      document_region_sdk_data_sst_(document_region_sdk_data_sst),
      document_cf_sst_meta_sdk_data_sst_(document_cf_sst_meta_sdk_data_sst) {}

RestoreSdkData::~RestoreSdkData() = default;

std::shared_ptr<RestoreSdkData> RestoreSdkData::GetSelf() { return shared_from_this(); }

butil::Status RestoreSdkData::Init() { return butil::Status::OK(); }

butil::Status RestoreSdkData::Run() { return butil::Status::OK(); }

butil::Status RestoreSdkData::Finish() { return butil::Status::OK(); }

butil::Status RestoreSdkData::CheckStoreRegionSdkDataSst() {
  butil::Status status;

  status = CheckRegionSdkDataSst(store_region_sdk_data_sst_, dingodb::Constant::kStoreRegionSdkDataSstName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}
butil::Status RestoreSdkData::CheckStoreCfSstMetaSdkDataSst() {
  butil::Status status;

  status = CheckCfSstMetaSdkDataSst(store_cf_sst_meta_sdk_data_sst_, dingodb::Constant::kStoreCfSstMetaSdkDataSstName,
                                    dingodb::Constant::kStoreRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkData::CheckIndexRegionSdkDataSst() {
  butil::Status status;

  status = CheckRegionSdkDataSst(index_region_sdk_data_sst_, dingodb::Constant::kIndexRegionSdkDataSstName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkData::CheckIndexCfSstMetaSdkDataSst() {
  butil::Status status;

  status = CheckCfSstMetaSdkDataSst(index_cf_sst_meta_sdk_data_sst_, dingodb::Constant::kIndexCfSstMetaSdkDataSstName,
                                    dingodb::Constant::kIndexRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkData::CheckDocumentRegionSdkDataSst() {
  butil::Status status;

  status = CheckRegionSdkDataSst(document_region_sdk_data_sst_, dingodb::Constant::kDocumentRegionSdkDataSstName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkData::CheckDocumentCfSstMetaSdkDataSst() {
  butil::Status status;

  status =
      CheckCfSstMetaSdkDataSst(document_cf_sst_meta_sdk_data_sst_, dingodb::Constant::kDocumentCfSstMetaSdkDataSstName,
                               dingodb::Constant::kDocumentRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkData::CheckRegionSdkDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> region_sdk_data_sst, const std::string& file_name) {
  butil::Status status;

  status = Utils::CheckBackupMeta(region_sdk_data_sst, storage_internal_, file_name, "",
                                  dingodb::Constant::kCoordinatorRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkData::CheckCfSstMetaSdkDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> cf_sst_meta_sdk_data_sst, const std::string& file_name,
    const std::string& exec_node) {
  butil::Status status;

  status = Utils::CheckBackupMeta(cf_sst_meta_sdk_data_sst, storage_internal_, file_name, "", exec_node);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

}  // namespace br