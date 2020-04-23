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
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/sbe_runtime_planner.h"

#include "mongo/db/exec/multi_plan.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/query/plan_executor_sbe.h"
#include "mongo/logv2/log.h"

namespace mongo::sbe {
namespace {
/**
 * Fetches a next document form the given plan stage tree and returns 'true' if the plan stage
 * returns EOF, or throws 'TrialRunProgressTracker::EarlyExitException' exception. Otherwise, the
 * loaded document is placed into the candidate's plan result queue.
 *
 * If the plan stage throws a 'DBException', it will be caught and the 'candidate->failed' flag
 * will be set to 'true', and the 'numFailures' parameter incremented by 1.
 */
bool fetchNextDocument(plan_ranker::CandidatePlan* candidate,
                       const std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>& slots,
                       size_t* numFailures) {
    try {
        BSONObj obj;
        RecordId recordId;

        auto [resultSlot, recordIdSlot] = slots;
        auto state = fetchNext(candidate->root.get(),
                               resultSlot,
                               recordIdSlot,
                               &obj,
                               recordIdSlot ? &recordId : nullptr);
        if (state == sbe::PlanState::IS_EOF) {
            candidate->root->close();
            return true;
        }

        invariant(state == sbe::PlanState::ADVANCED);
        candidate->results.push(
            {obj, recordIdSlot ? boost::make_optional(recordId) : boost::optional<RecordId>{}});
    } catch (TrialRunProgressTracker::EarlyExitException&) {
        candidate->exitedEarly = true;
        return true;
    } catch (DBException&) {
        candidate->root->close();
        candidate->failed = true;
        ++(*numFailures);
    }
    return false;
}
}  // namespace

std::pair<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>, bool>
BaseRuntimePlanner::prepareExecutionPlan(PlanStage* root,
                                         stage_builder::PlanStageData* data) const {
    invariant(root);
    invariant(data);

    root->prepare(data->ctx);

    sbe::value::SlotAccessor* resultSlot{nullptr};
    if (data->resultSlot) {
        resultSlot = root->getAccessor(data->ctx, *data->resultSlot);
        uassert(ErrorCodes::InternalError, "Query does not have result slot.", resultSlot);
    }

    sbe::value::SlotAccessor* recordIdSlot{nullptr};
    if (data->recordIdSlot) {
        recordIdSlot = root->getAccessor(data->ctx, *data->recordIdSlot);
        uassert(ErrorCodes::InternalError, "Query does not have record ID slot.", recordIdSlot);
    }

    root->attachFromOperationContext(_opCtx);

    auto exitedEarly{false};
    try {
        root->open(false);
    } catch (TrialRunProgressTracker::EarlyExitException&) {
        exitedEarly = true;
    }

    return {{resultSlot, recordIdSlot}, exitedEarly};
}

std::vector<plan_ranker::CandidatePlan> BaseRuntimePlanner::collectExecutionStats(
    std::vector<std::unique_ptr<QuerySolution>> solutions,
    std::vector<std::pair<std::unique_ptr<PlanStage>, stage_builder::PlanStageData>> roots) {
    invariant(solutions.size() == roots.size());

    ON_BLOCK_EXIT([this]() { _opCtx->stopTrialRun(); });

    auto maxNumResults{MultiPlanStage::getTrialPeriodNumToReturn(_cq)};
    auto maxNumReads{MultiPlanStage::getTrialPeriodWorks(_opCtx, _collection)};
    _opCtx->startTrialRun(maxNumResults, maxNumReads);

    std::vector<plan_ranker::CandidatePlan> candidates;
    std::vector<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>> slots;
    for (size_t ix = 0; ix < roots.size(); ++ix) {
        auto&& [root, data] = roots[ix];
        auto [accessors, exitedEarly] = prepareExecutionPlan(root.get(), &data);
        candidates.push_back(
            {std::move(solutions[ix]), std::move(root), std::move(data), exitedEarly});
        slots.push_back(accessors);
    }

    auto done{false};
    size_t numFailures{0};
    for (size_t it = 0; it < maxNumResults && !done; ++it) {
        for (size_t ix = 0; ix < candidates.size(); ++ix) {
            if (candidates[ix].failed || candidates[ix].exitedEarly) {
                continue;
            }

            done |= fetchNextDocument(&candidates[ix], slots[ix], &numFailures) ||
                (numFailures == candidates.size());
        }
    }

    // Make sure we have at least one plan which hasn't failed.
    uassert(2059102,
            "Runtime planner encountered a failure while collecting execution stats",
            numFailures != candidates.size());

    return candidates;
}
}  // namespace mongo::sbe
