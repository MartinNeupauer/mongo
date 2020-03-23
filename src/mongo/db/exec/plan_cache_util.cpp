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

#include "mongo/db/exec/plan_cache_util.h"
#include "mongo/db/query/explain.h"
#include "mongo/logv2/log.h"

namespace mongo {
namespace log_detail {
void logTieForBest(std::string query,
                   double winnerScore,
                   double runnerUpScore,
                   std::string winnerPlanSummary,
                   std::string runnerUpPlanSummary) {
    LOGV2_DEBUG(20594,
                1,
                "Winning plan tied with runner-up. Not caching. query: {query_Short} "
                "winner score: {ranking_scores_0} winner summary: "
                "{Explain_getPlanSummary_candidates_winnerIdx_root} runner-up score: "
                "{ranking_scores_1} runner-up summary: "
                "{Explain_getPlanSummary_candidates_runnerUpIdx_root}",
                "query_Short"_attr = redact(query),
                "ranking_scores_0"_attr = winnerScore,
                "Explain_getPlanSummary_candidates_winnerIdx_root"_attr = winnerPlanSummary,
                "ranking_scores_1"_attr = runnerUpScore,
                "Explain_getPlanSummary_candidates_runnerUpIdx_root"_attr = runnerUpPlanSummary);
}

void logNotCachingZeroResults(std::string query, double score, std::string winnerPlanSummary) {
    LOGV2_DEBUG(20595,
                1,
                "Winning plan had zero results. Not caching. query: {query_Short} winner "
                "score: {ranking_scores_0} winner summary: "
                "{Explain_getPlanSummary_candidates_winnerIdx_root}",
                "query_Short"_attr = redact(query),
                "ranking_scores_0"_attr = score,
                "Explain_getPlanSummary_candidates_winnerIdx_root"_attr = winnerPlanSummary);
}

void logNotCachingNoData(std::string solution) {
    LOGV2_DEBUG(20596,
                5,
                "Not caching query because this solution has no cache data: {solutions_ix}",
                "solutions_ix"_attr = redact(solution));
}
}  // namespace log_detail

void updatePlanCacheFeedback(const Collection* collection, const CanonicalQuery& cq, double score) {
    invariant(collection);

    auto cache = CollectionQueryInfo::get(collection).getPlanCache();
    auto fbs = cache->feedback(cq, score);
    if (!fbs.isOK()) {
        LOGV2_DEBUG(
            20583,
            5,
            "{canonicalQuery_ns}: Failed to update cache with feedback: {fbs} - (query: "
            "{canonicalQuery_getQueryObj}; sort: {canonicalQuery_getQueryRequest_getSort}; "
            "projection: {canonicalQuery_getQueryRequest_getProj}) is no longer in plan cache.",
            "canonicalQuery_ns"_attr = cq.ns(),
            "fbs"_attr = redact(fbs),
            "canonicalQuery_getQueryObj"_attr = redact(cq.getQueryObj()),
            "canonicalQuery_getQueryRequest_getSort"_attr = cq.getQueryRequest().getSort(),
            "canonicalQuery_getQueryRequest_getProj"_attr = cq.getQueryRequest().getProj());
    }
}
}  // namespace mongo
