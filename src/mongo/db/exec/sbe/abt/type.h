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
using RowsetId = int64_t;
/**
 * Type sort
 */
class NoType;
class VariantType;
class FunctionType;
class RowsetType;

using Type = algebra::PolyValue<NoType, VariantType, FunctionType, RowsetType>;

template <typename Derived, size_t Arity>
using TypeOperator = algebra::OpSpecificArity<Type, Derived, Arity>;

template <typename Derived, size_t Arity>
using TypeOperatorDynamic = algebra::OpSpecificDynamicArity<Type, Derived, Arity>;

template <typename T, typename... Args>
inline auto makeT(Args&&... args) {
    return Type::make<T>(std::forward<Args>(args)...);
}

class NoType final : public TypeOperator<NoType, 0> {
public:
    bool operator==(const NoType&) const noexcept {
        return true;
    }
};

// Variant type - "atomic" types only, does not admit function values
class VariantType final : public TypeOperator<VariantType, 0> {
public:
    bool operator==(const VariantType&) const noexcept {
        return true;
    }
};

class FunctionType final : public TypeOperator<FunctionType, 2> {
    using Base = TypeOperator<FunctionType, 2>;

public:
    FunctionType(Type lhs, Type rhs) : Base(std::move(lhs), std::move(rhs)) {}

    bool operator==(const FunctionType& rhs) const noexcept {
        return get<0>() == rhs.get<0>() && get<1>() == rhs.get<1>();
    }
};

class RowsetType final : public TypeOperator<RowsetType, 0> {
    RowsetId _id;
    // CONSIDER - should we track more info here (e.g. asc/desc direction?)
public:
    RowsetType(RowsetId id) : _id(id) {}
    bool operator==(const RowsetType& rhs) const noexcept {
        return _id == rhs._id;
    }
};

inline auto notype() {
    return makeT<NoType>();
}
inline auto varianttype() {
    return makeT<VariantType>();
}
inline auto rowsetType(RowsetId id) {
    return makeT<RowsetType>(id);
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo