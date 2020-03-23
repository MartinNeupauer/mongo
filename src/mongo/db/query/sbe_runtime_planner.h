/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/query_solution.h"
#include "mongo/db/query/sbe_plan_ranker.h"

namespace mongo::sbe {
/**
 * An interface to be implemented by all classes which can evaluate the cost for each pair of the
 * query solution and a plan stage tree in runtime by collecting execution stats for each of the
 * plans, and pick the best candidate plan according to certain criterion.
 */
class RuntimePlanner {
public:
    virtual ~RuntimePlanner() = default;

    virtual plan_ranker::CandidatePlan plan(std::vector<std::unique_ptr<QuerySolution>> solutions,
                                            std::vector<std::unique_ptr<PlanStage>> roots) = 0;
};

/**
 * A base class for runtime planner which provides a method to perform a trial run for the candidate
 * plan by executing each plan in a round-robin fashion and collecting execution stats. Each
 * specific implementation can use the collected stats to select the best plan amongst the
 * candidates.
 */
class BaseRuntimePlanner : public RuntimePlanner {
public:
    BaseRuntimePlanner(OperationContext* opCtx,
                       const Collection* collection,
                       const CanonicalQuery& cq)
        : _opCtx(opCtx), _collection(collection), _cq(cq) {
        invariant(_opCtx);
        invariant(_collection);
    }

protected:
    /**
     * Prepares the given plan stage tree for execution, attaches it to the operation context and
     * returns a pair of two slot accessors for the result and recordId slots, and a boolean value
     * indicating if the plan has exited early from the trial period.
     */
    std::pair<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>, bool>
    prepareExecutionPlan(PlanStage* root) const;

    /**
     * Executes each plan in a round-robin fashion to collect execution stats. Stops when:
     *  1) Any plan hits EOF.
     *  2) Or returns a pre-defined number of results.
     *  3) Or all candidate plans fail.
     *  4) Or a when a candidate plan exits early by throwing a special signaling exception.
     *
     * All documents returned by each plan are enqueued into the 'CandidatePlan->results' queue.
     *
     * Upon completion returns a vector of pairs of candidate plans. An execution stats can be
     * obtains for each of the candidate plan by calling 'CandidatePlan->root->getStats()'.
     *
     * After the trial period ends, all plans remain open.
     */
    std::vector<plan_ranker::CandidatePlan> collectExecutionStats(
        std::vector<std::unique_ptr<QuerySolution>> solutions,
        std::vector<std::unique_ptr<PlanStage>> roots);

    OperationContext* const _opCtx;
    const Collection* const _collection;
    const CanonicalQuery& _cq;
};
}  // namespace mongo::sbe
