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
#include "mongo/db/exec/sbe/abt/exe_generator.h"
#include "mongo/db/exec/sbe/abt/free_vars.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
ValueBinder::ValueBinder(std::vector<VarId> ids, std::vector<ABT> binds)
    : Base(std::move(binds)), _ids(std::move(ids)) {
    checkValueSyntaxSort(nodes());
    uassert(ErrorCodes::InternalError, "mismatched ids and binds", _ids.size() == nodes().size());
    for (size_t idx = 0; idx < _ids.size(); ++idx) {
        auto [it, inserted] = _indexes.emplace(_ids[idx], idx);
        uassert(ErrorCodes::InternalError, "duplicate variable id", inserted);
    }
}
ValueBinder::ValueBinder(const ValueBinder& other)
    : Base(other), _ids(other._ids), _indexes(other._indexes) {
    // TODO fix references
}

ValueBinder::~ValueBinder() {
    clear();
}

size_t ValueBinder::index(VarId id) const {
    auto it = _indexes.find(id);
    uassert(ErrorCodes::InternalError, "variable not found", it != _indexes.end());

    return it->second;
}
bool ValueBinder::isUsed(VarId id) const {
    auto idx = index(id);
    auto it = _references.find(id);
    // we will 'assume' that anything rooted in OptFence is 'used' and hence will not be optimized
    // away
    if (nodes()[idx].is<OptFence>() || it != _references.end()) {
        return true;
    }

    return false;
}
void ValueBinder::addReference(Variable* v) {
    // check that v is actually defined by this binder
    bool found = _indexes.count(v->id()) > 0;
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

/**
 * ExeGenerator
 */
ExeGenerator::GenResult ExeGenerator::walk(const ValueBinder& op, const std::vector<ABT>& binds) {
    auto it = _slots.find(&op);
    invariant(it != _slots.end());

    GenResult result;
    for (size_t idx = 0; idx < binds.size(); ++idx) {
        if (op.isUsed(op.ids()[idx])) {
            invariant(_currentStage);
            auto bindResult = generateBind(it->second[idx], binds[idx]);
            invariant(!_currentStage);
            _currentStage = std::move(bindResult.stage);
            result.exprs.emplace_back(std::move(bindResult.expr));
        } else {
            result.exprs.emplace_back(makeE<EConstant>(value::TypeTags::Nothing, 0));
        }
    }
    return result;
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
