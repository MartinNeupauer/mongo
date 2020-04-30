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

#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/query/plan_yield_policy_sbe.h"
#include "mongo/db/query/stage_builder.h"

namespace mongo::stage_builder {
struct PlanStageData {
    boost::optional<sbe::value::SlotId> resultSlot;
    boost::optional<sbe::value::SlotId> recordIdSlot;
    boost::optional<sbe::value::SlotId> oplogTsSlot;
    sbe::CompileCtx ctx;
    bool shouldTrackLatestOplogTimestamp{false};
    bool shouldTrackResumeToken{false};

    std::string debug() {
        StringBuilder builder;

        if (resultSlot) {
            builder << "$$RESULT=s" << *resultSlot << " ";
        }
        if (recordIdSlot) {
            builder << "$$RID=s" << *recordIdSlot << " ";
        }
        if (oplogTsSlot) {
            builder << "$$OPLOGTS=s" << *oplogTsSlot << " ";
        }

        return builder.str();
    }
};

/**
 * A stage builder which builds an executable tree using slot-based PlanStages.
 */
class SlotBasedStageBuilder : public StageBuilder<sbe::PlanStage> {
public:
    SlotBasedStageBuilder(OperationContext* opCtx,
                          const Collection* collection,
                          const CanonicalQuery& cq,
                          const QuerySolution& solution,
                          PlanYieldPolicySBE* yieldPolicy)
        : StageBuilder(opCtx, collection, cq, solution, nullptr), _yieldPolicy(yieldPolicy) {}

    std::unique_ptr<sbe::PlanStage> build(const QuerySolutionNode* root) final;

    PlanStageData getPlanStageData() const {
        return _data;
    }

private:
    std::unique_ptr<sbe::PlanStage> buildCollScan(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildIndexScan(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildFetch(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildLimit(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildSkip(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildSort(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildSortKeyGeneraror(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildProjectionSimple(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildProjectionDefault(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildOr(const QuerySolutionNode* root);
    std::unique_ptr<sbe::PlanStage> buildText(const QuerySolutionNode* root);

    std::unique_ptr<sbe::PlanStage> makeLoopJoinForFetch(std::unique_ptr<sbe::PlanStage> inputStage,
                                                         sbe::value::SlotId recordIdKeySlot);

    sbe::value::SlotIdGenerator _slotIdGenerator;
    sbe::value::FrameIdGenerator _frameIdGenerator;
    sbe::value::SpoolIdGenerator _spoolIdGenerator;

    boost::optional<long long> _limit;

    PlanYieldPolicySBE* const _yieldPolicy;

    PlanStageData _data;
};
}  // namespace mongo::stage_builder
