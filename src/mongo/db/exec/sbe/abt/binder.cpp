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

#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/abt/free_vars.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
ValueBinder::ValueBinder(std::vector<VarId> ids, std::vector<ABT> binds)
    : Base(std::move(binds)), _ids(std::move(ids)) {
    checkValueSyntaxSort(nodes());
    uassert(ErrorCodes::InternalError, "mismatched ids and binds", _ids.size() == nodes().size());
}
ValueBinder::~ValueBinder() {
    clear();
}
void ValueBinder::addReference(Variable* v) {
    // check that v is actually defined by this binder
    bool found = false;
    for (auto id : _ids) {
        if (id == v->id()) {
            found = true;
            break;
        }
    }
    uassert(ErrorCodes::InternalError, "unknown variable", found);

    auto [it, inserted] = _references[v->id()].emplace(v);
    uassert(ErrorCodes::InternalError, "duplicate variable reference", inserted);
}

void ValueBinder::removeReference(Variable* v) {
    auto it = _references.find(v->id());
    uassert(ErrorCodes::InternalError, "unknown variable", it != _references.end());

    auto itSet = it->second.find(v);
    uassert(ErrorCodes::InternalError, "unknown variable", itSet != it->second.end());
    it->second.erase(itSet);

    if (it->second.empty()) {
        _references.erase(it);
    }
}

void ValueBinder::clear() {
    if (!_references.empty()) {
        // make a copy for stability
        auto copy = _references;

        for (auto&& [id, varSet] : copy) {
            for (auto v : varSet) {
                v->rebind(nullptr);
            }
        }
    }
}
/**
 * Free variables
 */
ABT* FreeVariables::transport(ABT& e, ValueBinder& op, std::vector<ABT*> binds) {
    // make the transport idempotent.
    op.clear();

    for (size_t idx = 0; idx < binds.size(); ++idx) {
        // pull out free and defined variables for a child
        mergeFreeVars(&e, binds[idx]);
        mergeDefinedVars(&e, binds[idx]);

        // resolve free variables against current set of defined variables
        resolveVars(&e, &e);

        // check for circular dependency; i.e. we are trying to define a variable that we are
        // already dependent on
        uassert(ErrorCodes::InternalError, "Circular dependency", !isFreeVar(&e, op.ids()[idx]));

        // and finally define a new variable
        addDefinedVar(&e, op.ids()[idx]);
    }
    return &e;
}

}  // namespace abt
}  // namespace sbe
}  // namespace mongo
