/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/exec/sbe/abt/base.h"
#include "mongo/db/namespace_string.h"

#include <boost/optional.hpp>

namespace mongo {
namespace sbe {
namespace abt {
class Scan final : public Operator<Scan, 1>, public OpSyntaxSort {
    using Base = Operator<Scan, 1>;

    const VarId _rowsetVar;
    const VarId _outputVar;
    const boost::optional<NamespaceStringOrUUID> _name;

    // TODO - this is temporary from MH bson file scan
    const char* const _bsonBegin{nullptr};
    const char* const _bsonEnd{nullptr};

public:
    const auto& name() const {
        return _name;
    }
    auto bsonBegin() const {
        return _bsonBegin;
    }
    auto bsonEnd() const {
        return _bsonEnd;
    }

    auto rowsetVar() const {
        return _rowsetVar;
    }
    auto outputVar() const {
        return _outputVar;
    }
    ABT& body() override {
        return get<0>();
    }

    Scan(const NamespaceStringOrUUID& name, VarId rowsetVar, VarId outputVar, ABT bodyIn);
    Scan(const char* bsonBegin, const char* bsonEnd, VarId rowsetVar, VarId outputVar, ABT bodyIn);
};
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
