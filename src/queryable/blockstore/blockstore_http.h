/**
 * Copyright (C) 2018 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include "mongo/base/data_builder.h"
#include "mongo/base/data_range.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/oid.h"
#include "mongo/util/net/http_client.h"

namespace mongo {
namespace queryable {

const char* const kSecretKeyEnvVar = "SECRET_KEY";

class BlockstoreHTTP {
public:
    BlockstoreHTTP(StringData apiUrl,
                   mongo::OID snapshotId,
                   std::unique_ptr<HttpClient> client = std::unique_ptr<HttpClient>());

    StatusWith<std::size_t> read(StringData path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const;
    StatusWith<DataBuilder> listDirectory() const;

private:
    std::string _apiUrl;
    mongo::OID _snapshotId;
    std::unique_ptr<HttpClient> _client;
};

}  // namespace queryable
}  // namespace mongo
