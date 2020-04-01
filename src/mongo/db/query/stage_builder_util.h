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

#include "mongo/db/query/classic_stage_builder.h"
#include "mongo/db/query/sbe_stage_builder.h"

namespace mongo::stage_builder {
/**
 * Turns 'solution' into an executable tree of PlanStage(s). Returns a pointer to the root of
 * the plan stage tree.
 *
 * 'cq' must be the CanonicalQuery from which 'solution' is derived. Illegal to call if 'ws'
 * is nullptr, or if 'solution.root' is nullptr.
 *
 * The 'PlanStageType' type parameter defines a specifc type of PlanStage the executable tree
 * will consist of.
 */
template <typename PlanStageType>
std::unique_ptr<PlanStageType> buildExecutableTree(OperationContext* opCtx,
                                                   const Collection* collection,
                                                   const CanonicalQuery& cq,
                                                   const QuerySolution& solution,
                                                   WorkingSet* ws = nullptr) {
    // Only QuerySolutions derived from queries parsed with context, or QuerySolutions derived from
    // queries that disallow extensions, can be properly executed. If the query does not have
    // $text/$where context (and $text/$where are allowed), then no attempt should be made to
    // execute the query.
    invariant(!cq.canHaveNoopMatchNodes());

    invariant(solution.root);

    std::unique_ptr<StageBuilder<PlanStageType>> builder;
    if constexpr (std::is_same_v<PlanStageType, sbe::PlanStage>) {
        builder = std::make_unique<SlotBasedStageBuilder>(opCtx, collection, cq, solution);
    } else {
        invariant(ws);
        builder = std::make_unique<ClassicStageBuilder>(opCtx, collection, cq, solution, ws);
    }
    return builder->build(solution.root.get());
}
}  // namespace mongo::stage_builder
