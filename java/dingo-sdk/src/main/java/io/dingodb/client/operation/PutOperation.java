/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.client.operation;

import io.dingodb.client.ContextForStore;
import io.dingodb.client.IStoreOperation;
import io.dingodb.client.ResultForStore;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.service.store.StoreServiceClient;

public class PutOperation implements IStoreOperation {

    private static final PutOperation INSTANCE = new PutOperation();

    private PutOperation() {
    }

    public static PutOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public ResultForStore doOperation(DingoCommonId tableId, StoreServiceClient storeServiceClient, ContextForStore contextForStore) {
        boolean isSuccess = storeServiceClient.kvBatchPut(tableId, contextForStore.getRegionId(), contextForStore.getRecordList());
        return new ResultForStore(isSuccess ? 0 : -1, "");
    }
}
