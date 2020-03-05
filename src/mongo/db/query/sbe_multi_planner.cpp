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

#include "mongo/db/query/sbe_multi_planner.h"

#include "mongo/db/exec/multi_plan.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/query/explain.h"
#include "mongo/db/query/plan_executor_sbe.h"
#include "mongo/logv2/log.h"

namespace mongo::sbe::multi_planner {
namespace {
/**
 * Prepares the given SBE plan stage trees in the 'planRoots' vector for execution and returns
 * a pair of vectors, one holding CandidatePlans instances, corresponding to each plan stage tree,
 * and another one pairs of result and recordId slot accessors for the same tree.
 *
 * Each plan stage tree is attached to the operation context, and the 'open' method is called.
 */
std::pair<std::vector<plan_ranker::CandidatePlan<sbe::PlanStage,
                                                 std::pair<BSONObj, boost::optional<RecordId>>>>,
          std::vector<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>>>
prepareCandidatePlans(OperationContext* opCtx,
                      const CanonicalQuery& canonicalQuery,
                      std::vector<std::unique_ptr<QuerySolution>> querySolutions,
                      std::vector<std::unique_ptr<sbe::PlanStage>>& planRoots) {
    std::vector<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>> slotAccessors;
    std::vector<
        plan_ranker::CandidatePlan<sbe::PlanStage, std::pair<BSONObj, boost::optional<RecordId>>>>
        candidates;
    for (size_t ix = 0; ix < planRoots.size(); ++ix) {
        candidates.push_back({std::move(querySolutions[ix]), planRoots[ix].get()});
    }

    for (auto&& root : planRoots) {
        sbe::CompileCtx ctx;
        root->prepare(ctx);

        auto resultSlot = root->getAccessor(ctx, sbe::value::SystemSlots::kResultSlot);
        uassert(ErrorCodes::InternalError, "Query does not have result slot.", resultSlot);

        sbe::value::SlotAccessor* resultRecordId{nullptr};
        if (canonicalQuery.metadataDeps()[DocumentMetadataFields::kRecordId]) {
            resultRecordId = root->getAccessor(ctx, sbe::value::SystemSlots::kRecordIdSlot);
            uassert(
                ErrorCodes::InternalError, "Query does not have record ID slot.", resultRecordId);
        }

        root->attachFromOperationContext(opCtx);
        root->open(false);

        slotAccessors.push_back({resultSlot, resultRecordId});
    }
    return {std::move(candidates), std::move(slotAccessors)};
}

/**
 * Fetches a next document form the given plan stage tree and returns 'true' if the plan stage
 * returns EOF. Otherwise, the loaded document is placed into the 'candidate' result queue.
 *
 * If the plan stage throws a 'DBException', it will be caught and the 'candidate->failed' flag
 * will be set to 'true', and the 'numFailures' parameter incremented by 1.
 */
bool fetchNextDocument(
    sbe::PlanStage* root,
    sbe::value::SlotAccessor* resultSlot,
    sbe::value::SlotAccessor* recordIdSlot,
    plan_ranker::CandidatePlan<sbe::PlanStage, std::pair<BSONObj, boost::optional<RecordId>>>*
        candidate,
    size_t* numFailures) {
    try {
        BSONObj obj;
        RecordId recordId;

        if (fetchNext(root, resultSlot, recordIdSlot, &obj, recordIdSlot ? &recordId : nullptr) ==
            sbe::PlanState::IS_EOF) {
            root->close();
            return true;
        }

        candidate->results.push(
            {obj, recordIdSlot ? boost::make_optional(recordId) : boost::optional<RecordId>{}});
    } catch (DBException&) {
        candidate->failed = true;
        ++(*numFailures);
    }
    return false;
}

/**
 * Executes each candidate plan in a round-robin fashion to collect execution stats. Stops when any
 * plan hits EOF, or returns a pre-defined number of results, or candidate plans fail. All documents
 * returned by each plan are enqueued into the 'candidate->results' queue.
 *
 * Upon completion returns a vector of plan stage stat trees, for each of the candidate plans.
 */
std::vector<std::unique_ptr<sbe::PlanStageStats>> collectExecutionStats(
    OperationContext* opCtx,
    const CanonicalQuery& canonicalQuery,
    Collection* collection,
    std::vector<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>>& slotAccessors,
    std::vector<
        plan_ranker::CandidatePlan<sbe::PlanStage, std::pair<BSONObj, boost::optional<RecordId>>>>&
        candidates) {
    invariant(opCtx);
    invariant(candidates.size() == slotAccessors.size());

    auto done{false};
    auto numResults{MultiPlanStage::getTrialPeriodNumToReturn(canonicalQuery)};
    auto numReads{MultiPlanStage::getTrialPeriodWorks(opCtx, collection)};
    size_t numFailures{0};
    for (size_t it = 0; it < numReads && !done; ++it) {
        for (size_t ix = 0; ix < candidates.size() && !done; ++ix) {
            if (!candidates[ix].failed) {
                auto [resultSlot, recordIdSlot] = slotAccessors[ix];
                done = fetchNextDocument(candidates[ix].root,
                                         resultSlot,
                                         recordIdSlot,
                                         &candidates[ix],
                                         &numFailures) ||
                    (numResults == candidates[ix].results.size()) ||
                    (numFailures == candidates.size());
            }
        }
    }

    std::vector<std::unique_ptr<sbe::PlanStageStats>> stats;
    for (auto&& candidate : candidates) {
        stats.push_back(candidate.root->getStats());
    }
    return stats;
}

/**
 * Returns the plan stage tree from the 'planRoots' vector with the best rank according to the
 * plan ranking 'decision', along with the slot accessors for the result and recordId slots, and
 * as a queue containing documents and recordId's obtained during the trial run by the returned
 * plan stage tree.
 *
 * Calls 'close' method on all other candidate plans.
 */
std::tuple<std::unique_ptr<sbe::PlanStage>,
           sbe::value::SlotAccessor*,
           sbe::value::SlotAccessor*,
           std::queue<std::pair<BSONObj, boost::optional<RecordId>>>>
finalizeExecutionPlans(
    std::vector<std::unique_ptr<sbe::PlanStage>> planRoots,
    std::vector<std::pair<sbe::value::SlotAccessor*, sbe::value::SlotAccessor*>> slotAccessors,
    std::unique_ptr<plan_ranker::PlanRankingDecision<PlanStageStats>> decision,
    std::vector<
        plan_ranker::CandidatePlan<sbe::PlanStage, std::pair<BSONObj, boost::optional<RecordId>>>>
        candidates) {
    invariant(decision);
    invariant(planRoots.size() == slotAccessors.size());

    const auto bestPlanIdx = decision->candidateOrder[0];
    invariant(bestPlanIdx >= 0 && bestPlanIdx < planRoots.size());

    LOGV2_DEBUG(2059001,
                5,
                "Winning solution:\n{bestSolution}",
                "bestSolution"_attr = redact(candidates[bestPlanIdx].solution->toString()));
    LOGV2_DEBUG(2059101,
                2,
                "Winning plan: {Explain_getPlanSummary_bestCandidate_root}",
                "Explain_getPlanSummary_bestCandidate_root"_attr =
                    Explain::getPlanSummary(candidates[bestPlanIdx].root));

    for (size_t ix = 1; ix < decision->candidateOrder.size(); ++ix) {
        const auto planIdx = decision->candidateOrder[ix];
        invariant(planIdx >= 0 && planIdx < planRoots.size());
        planRoots[planIdx]->close();
    }

    return {std::move(planRoots[bestPlanIdx]),
            slotAccessors[bestPlanIdx].first,
            slotAccessors[bestPlanIdx].second,
            std::move(candidates[bestPlanIdx].results)};
}
}  // namespace

std::tuple<std::unique_ptr<sbe::PlanStage>,
           sbe::value::SlotAccessor*,
           sbe::value::SlotAccessor*,
           std::queue<std::pair<BSONObj, boost::optional<RecordId>>>>
pickBestPlan(OperationContext* opCtx,
             const CanonicalQuery& canonicalQuery,
             Collection* collection,
             std::vector<std::unique_ptr<QuerySolution>> querySolutions,
             std::vector<std::unique_ptr<sbe::PlanStage>> planRoots) {
    auto&& [candidates, slotAccessors] =
        prepareCandidatePlans(opCtx, canonicalQuery, std::move(querySolutions), planRoots);
    auto stats =
        collectExecutionStats(opCtx, canonicalQuery, collection, slotAccessors, candidates);
    auto decision = uassertStatusOK(plan_ranker::pickBestPlan(std::move(stats), candidates));
    return finalizeExecutionPlans(
        std::move(planRoots), std::move(slotAccessors), std::move(decision), std::move(candidates));
}
}  // namespace mongo::sbe::multi_planner
