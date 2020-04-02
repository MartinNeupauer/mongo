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
#include "mongo/db/exec/sbe/abt/abt.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mongo {
namespace sbe {
namespace abt {
/**
 * Free variables tracking
 */
class FreeVariables {
    using VarMultiSet = std::unordered_multimap<VarId, Variable*>;
    using BindVarMap = std::unordered_map<VarId, ValueBinder*>;

    // free variables
    std::unordered_map<ABT*, VarMultiSet> _freeVars;

    // defined variables
    std::unordered_map<ABT*, BindVarMap> _definedVars;

    void addFreeVar(ABT* e);
    void mergeFreeVars(ABT* current, ABT* other);

    void addDefinedVar(ABT* e, VarId id);
    void mergeDefinedVars(ABT* current, ABT* other);
    void resetDefinedVars(ABT* e);

    void resolveVars(ABT* free, ABT* def);
    bool isFreeVar(ABT* e, VarId id);

    void mergeVarsHelper(ABT* current, ABT* other);
    void mergeVarsHelper(ABT* current, std::vector<ABT*>& other);

public:
    void compute(ABT& e) {
        algebra::transport<true>(e, *this);
    }
    bool hasFreeVars() const {
        return !_freeVars.empty();
    }
    ABT* transport(ABT& e, Constant& op);
    ABT* transport(ABT& e, ConstantMagic& op);
    ABT* transport(ABT& e, Variable& op);
    ABT* transport(ABT& e, EvalPath& op, ABT* path, ABT* input);
    ABT* transport(ABT& e, FDep& op, std::vector<ABT*> deps);
    ABT* transport(ABT& e, PathIdentity& op);
    ABT* transport(ABT& e, PathConstant& op, ABT* c);
    ABT* transport(ABT& e, PathLambda& op, ABT* c);
    ABT* transport(ABT& e, PathDrop& op);
    ABT* transport(ABT& e, PathKeep& op);
    ABT* transport(ABT& e, PathObj& op);
    ABT* transport(ABT& e, PathTraverse& op, ABT* c);
    ABT* transport(ABT& e, PathField& op, ABT* c);
    ABT* transport(ABT& e, PathGet& op, ABT* c);
    ABT* transport(ABT& e, PathCompose& op, ABT* t2, ABT* t1);
    ABT* transport(ABT& e, Scan& op, ABT* body);
    ABT* transport(ABT& e, Unwind& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, Join& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, Filter& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, Group& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, Facet& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, Sort& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, Exchange& op, std::vector<ABT*> deps, ABT* body);
    ABT* transport(ABT& e, ValueBinder& op, std::vector<ABT*> binds);
};
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
