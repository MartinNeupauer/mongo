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
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"

namespace mongo {
namespace sbe {
namespace abt {
class ValueBinder final : public OperatorDynamic<ValueBinder, 0> {
    using Base = OperatorDynamic<ValueBinder, 0>;
    std::vector<VarId> _ids;
    stdx::unordered_map<VarId, size_t> _indexes;

    // TODO - copying ValueBinder is problematic
    stdx::unordered_map<VarId, stdx::unordered_set<Variable*>> _references;

    void addReference(Variable* v);
    void removeReference(Variable* v);

    // Only variables can manipulate references
    friend class Variable;

public:
    auto& ids() const {
        return _ids;
    }
    auto& binds() const {
        return nodes();
    }
    auto& binds() {
        return nodes();
    }
    size_t index(VarId id) const;
    bool isUsed(VarId id) const;

    ValueBinder(std::vector<VarId> ids, std::vector<ABT> binds);
    ValueBinder(const ValueBinder& other);
    ~ValueBinder();

    void clear();
};

namespace detail {
// base case
inline auto makeBinder_unwind(std::vector<VarId>& ids, std::vector<ABT>& binds) {
    /*ids.emplace_back(id);
    binds.emplace_back(std::move(bind));*/

    return make<ValueBinder>(std::move(ids), std::move(binds));
}
// recursive case
template <typename... Ts>
inline auto makeBinder_unwind(
    std::vector<VarId>& ids, std::vector<ABT>& binds, VarId id, ABT bind, Ts&&... rest) {
    ids.emplace_back(id);
    binds.emplace_back(std::move(bind));

    return makeBinder_unwind(ids, binds, std::forward<Ts>(rest)...);
}
}  // namespace detail

template <typename... Ts>
inline auto makeBinder(Ts&&... pack) {
    std::vector<VarId> ids;
    std::vector<ABT> binds;
    return detail::makeBinder_unwind(ids, binds, std::forward<Ts>(pack)...);
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
