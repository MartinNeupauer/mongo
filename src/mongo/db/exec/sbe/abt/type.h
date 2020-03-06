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

namespace mongo {
namespace sbe {
namespace abt {
class NoType : public Operator<NoType, 0>, public TypeSyntaxSort {
public:
    bool operator==(const NoType&) const {
        return true;
    }
};

// Variant type - "atomic" types only, does not admit function values
class VariantType : public Operator<VariantType, 0>, public TypeSyntaxSort {
public:
    bool operator==(const NoType&) const {
        return true;
    }
};

class FunctionType : public Operator<FunctionType, 2>, public TypeSyntaxSort {
    using Base = Operator<FunctionType, 2>;

public:
    FunctionType(ABT lhs, ABT rhs) : Base(std::move(lhs), std::move(rhs)) {}
};

inline auto notype() {
    return make<NoType>();
}
inline auto varianttype() {
    return make<VariantType>();
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo