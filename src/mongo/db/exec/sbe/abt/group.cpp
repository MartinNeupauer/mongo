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
#include "mongo/db/exec/sbe/stages/hash_agg.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
/**
 * Free variables
 */
ABT* FreeVariables::transport(ABT& e, Group& op, std::vector<ABT*> deps, ABT* body) {
    mergeVarsHelper(&e, deps);
    uassert(ErrorCodes::InternalError, "Group has free variables", !hasFreeVars(&e));

    mergeFreeVars(&e, body);

    // resolve free variables against current set of defined variables
    resolveVars(&e, &e);

    // no defined variables from below the group are accessible from above
    resetDefinedVars(&e);

    // only variables defined in the body are accessible
    mergeDefinedVars(&e, body);
    resolveVars(&e, &e);

    return &e;
}

/**
 * ExeGenerator
 */
ExeGenerator::GenResult ExeGenerator::walk(const Group& op,
                                           const std::vector<ABT>& deps,
                                           const ABT& body) {
    auto resultDeps = generateDeps(deps);
    invariant(resultDeps.size() == 1);
    invariant(!_currentStage);

    _currentStage = std::move(resultDeps[0].stage);
    invariant(_currentStage);

    auto resultInput = generateInputPhase(op.rowsetVar(), body);
    _currentStage = std::move(resultInput.stage);

    auto binder = body.cast<ValueBinder>();
    value::SlotVector gbs;
    for (auto gb : op.gbs()) {
        gbs.push_back(getSlot(binder, gb));
    }
    value::SlotMap<std::unique_ptr<EExpression>> aggs;

    _currentStage = makeS<HashAggStage>(std::move(_currentStage), gbs, std::move(aggs));

    auto resultOutput = generateOutputPhase(op.rowsetVar(), body);

    GenResult result;
    result.stage = std::move(resultOutput.stage);
    return result;
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
