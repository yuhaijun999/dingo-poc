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

#ifndef DINGODB_BR_RESTORE_SDK_DATA_H_
#define DINGODB_BR_RESTORE_SDK_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class RestoreSdkData : public std::enable_shared_from_this<RestoreSdkData> {
 public:
  RestoreSdkData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                 ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                 const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                 const std::string& storage_internal,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sdk_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sdk_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sdk_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sdk_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sdk_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sdk_data_sst);
  ~RestoreSdkData();

  RestoreSdkData(const RestoreSdkData&) = delete;
  const RestoreSdkData& operator=(const RestoreSdkData&) = delete;
  RestoreSdkData(RestoreSdkData&&) = delete;
  RestoreSdkData& operator=(RestoreSdkData&&) = delete;

  std::shared_ptr<RestoreSdkData> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status CheckStoreRegionSdkDataSst();
  butil::Status CheckStoreCfSstMetaSdkDataSst();
  butil::Status CheckIndexRegionSdkDataSst();
  butil::Status CheckIndexCfSstMetaSdkDataSst();
  butil::Status CheckDocumentRegionSdkDataSst();
  butil::Status CheckDocumentCfSstMetaSdkDataSst();
  butil::Status CheckRegionSdkDataSst(std::shared_ptr<dingodb::pb::common::BackupMeta> region_sdk_data_sst,
                                      const std::string& file_name);
  butil::Status CheckCfSstMetaSdkDataSst(std::shared_ptr<dingodb::pb::common::BackupMeta> cf_sst_meta_sdk_data_sst,
                                         const std::string& file_name, const std::string& exec_node);
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;

  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  // sdk maybe nullptr. if no any data.
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sdk_data_sst_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_SDK_DATA_H_