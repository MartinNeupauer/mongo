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

#include "mongo/db/exec/sbe/abt/free_vars.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
void FreeVariables::addFreeVar(ABT* e) {
    uassert(ErrorCodes::InternalError, "Variable expected", e->is<Variable>());
    VarMultiSet m;
    m.emplace(e->cast<Variable>()->id(), e->cast<Variable>());
    auto [it, inserted] = _freeVars.emplace(e, std::move(m));

    uassert(ErrorCodes::InternalError, "Duplicate variable", inserted);
}
void FreeVariables::mergeFreeVars(ABT* current, ABT* other) {
    auto it = _freeVars.find(other);
    if (it == _freeVars.end())
        return;
    auto& v = _freeVars[current];
    v.merge(it->second);
    _freeVars.erase(it);
}

void FreeVariables::addDefinedVar(ABT* e, VarId id) {
    uassert(ErrorCodes::InternalError, "Variable expected", e->is<ValueBinder>());
    auto [it, inserted] = _definedVars[e].emplace(id, e->cast<ValueBinder>());

    uassert(ErrorCodes::InternalError, "Duplicate variable definition", inserted);
}

void FreeVariables::mergeDefinedVars(ABT* current, ABT* other) {
    auto it = _definedVars.find(other);
    if (it == _definedVars.end())
        return;

    auto& v = _definedVars[current];
    for (auto&& [id, bind] : it->second) {
        auto [it, inserted] = v.emplace(id, bind);
        uassert(ErrorCodes::InternalError, "Duplicate variable definition", inserted);
    }
    _definedVars.erase(it);
}

void FreeVariables::resetDefinedVars(ABT* e) {
    _definedVars.erase(e);
}

void FreeVariables::resolveVars(ABT* free, ABT* def) {
    auto itFree = _freeVars.find(free);
    if (itFree == _freeVars.end()) {
        // no free variables to resolve so bail out
        return;
    }

    auto itDef = _definedVars.find(def);
    if (itDef == _definedVars.end()) {
        // no defined variables so bail out
        return;
    }

    for (auto&& [id, bind] : itDef->second) {
        auto [itB, itE] = itFree->second.equal_range(id);
        for (auto itC = itB; itC != itE; ++itC) {
            itC->second->rebind(bind);
        }
        itFree->second.erase(itB, itE);
    }

    if (itFree->second.empty()) {
        _freeVars.erase(itFree);
    }
}

// A common helper that merges and resolves variables.
void FreeVariables::mergeVarsHelper(ABT* current, ABT* other) {
    // pull out free and defined variables
    mergeFreeVars(current, other);
    mergeDefinedVars(current, other);

    // resolve free variables against current set of defined variables
    resolveVars(current, current);
}
void FreeVariables::mergeVarsHelper(ABT* current, const std::vector<ABT*>& other) {
    for (auto o : other) {
        mergeVarsHelper(current, o);
    }
}

bool FreeVariables::isFreeVar(ABT* e, VarId id) {
    if (auto it = _freeVars.find(e); it != _freeVars.end()) {
        return it->second.count(id) > 0;
    }

    return false;
}

}  // namespace abt
}  // namespace sbe
}  // namespace mongo