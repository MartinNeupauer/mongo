/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "http_client.h"

#include "mongo/base/data_builder.h"

namespace mongo {
namespace queryable {

class CurlHttpClient final : public HttpClientBase {
public:
    CurlHttpClient(std::string apiUri, OID snapshotId);

    StatusWith<std::size_t> read(std::string path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const override;

    StatusWith<DataBuilder> listDirectory() const override;
};

}  // namespace queryable
}  // namespace mongo
