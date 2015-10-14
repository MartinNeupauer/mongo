// artree_init.cpp

/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_options.h"

#include "artree_kv_engine.h"

namespace mongo {

namespace {

class ARTreeFactory : public StorageEngine::Factory {
public:
    virtual ~ARTreeFactory() {}

    virtual StorageEngine* create(const StorageGlobalParams& params,
                                  const StorageEngineLockFile& lockFile) const {
        KVStorageEngineOptions options;
        // TODO: error out when inapplicable parameters are passed in
        options.directoryPerDB = params.directoryperdb;
        options.forRepair = params.repair;
        KVEngine* engine = new ARTreeKVEngine();
        return new KVStorageEngine(engine, options);
    }

    virtual StringData getCanonicalName() const {
        return "inMemory";
    }

    virtual Status validateCollectionStorageOptions(const BSONObj& options) const {
        return Status::OK();
    }

    virtual Status validateIndexStorageOptions(const BSONObj& options) const {
        return Status::OK();
    }

    virtual Status validateMetadata(const StorageEngineMetadata& metadata,
                                    const StorageGlobalParams& params) const {
        return Status::OK();
    }

    virtual BSONObj createMetadataOptions(const StorageGlobalParams& params) const {
        return BSONObj();
    }
};

}  // namespace

MONGO_INITIALIZER_WITH_PREREQUISITES(ARTreeEngineInit, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine("inMemory", new ARTreeFactory());
    return Status::OK();
}

}  // namespace mongo
