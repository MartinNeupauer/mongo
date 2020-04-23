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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/sbe_stage_builder.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/hash_agg.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/makeobj.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/exec/sbe/stages/union.h"
#include "mongo/db/query/sbe_stage_builder_coll_scan.h"
#include "mongo/db/query/sbe_stage_builder_filter.h"
#include "mongo/db/query/sbe_stage_builder_index_scan.h"
#include "mongo/db/query/sbe_stage_builder_projection.h"
#include "mongo/db/query/util/make_data_structure.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

namespace mongo::stage_builder {
std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildCollScan(
    const QuerySolutionNode* root) {
    auto csn = static_cast<const CollectionScanNode*>(root);
    auto [resultSlot, recordIdSlot, oplogTsSlot, stage] =
        generateCollScan(_opCtx, _collection, csn, &_slotIdGenerator, _yieldPolicy);
    _data.resultSlot = resultSlot;
    _data.recordIdSlot = recordIdSlot;
    _data.oplogTsSlot = oplogTsSlot;
    _data.shouldTrackLatestOplogTimestamp = csn->shouldTrackLatestOplogTimestamp;
    _data.shouldTrackResumeToken = csn->requestResumeToken;
    return std::move(stage);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildIndexScan(
    const QuerySolutionNode* root) {
    auto ixn = static_cast<const IndexScanNode*>(root);
    auto [slot, stage] = generateIndexScan(
        _opCtx, _collection, ixn, &_slotIdGenerator, &_spoolIdGenerator, _yieldPolicy);
    _data.recordIdSlot = slot;
    return std::move(stage);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildFetch(const QuerySolutionNode* root) {
    auto fn = static_cast<const FetchNode*>(root);
    auto inputStage = build(fn->children[0]);

    uassert(ErrorCodes::InternalError, "RecordId slot is not defined", _data.recordIdSlot);

    auto recordIdKeySlot = _data.recordIdSlot;
    _data.resultSlot = _slotIdGenerator.generate();
    _data.recordIdSlot = _slotIdGenerator.generate();

    // Scan the collection in the range [recordIdKeySlot, Inf).
    auto collScan = sbe::makeS<sbe::ScanStage>(
        NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
        _data.resultSlot,
        _data.recordIdSlot,
        std::vector<std::string>{},
        std::vector<sbe::value::SlotId>{},
        recordIdKeySlot,
        true,
        nullptr);

    // Get the recordIdKeySlot from the outer side (e.g., IXSCAN) and feed it to the inner side,
    // limiting the result set to 1 row.
    auto stage = sbe::makeS<sbe::LoopJoinStage>(
        std::move(inputStage),
        sbe::makeS<sbe::LimitSkipStage>(std::move(collScan), 1, boost::none),
        std::vector<sbe::value::SlotId>{},
        std::vector<sbe::value::SlotId>{*recordIdKeySlot},
        nullptr);

    if (fn->filter) {
        stage = generateFilter(
            fn->filter.get(), std::move(stage), &_slotIdGenerator, *_data.resultSlot);
    }

    return stage;
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildLimit(const QuerySolutionNode* root) {
    const auto ln = static_cast<const LimitNode*>(root);
    // If we have both limit and skip stages and the skip stage is beneath the limit, then we can
    // combine these two stages into one. So, save the _limit value and let the skip stage builder
    // handle it.
    if (ln->children[0]->getType() == StageType::STAGE_SKIP) {
        _limit = ln->limit;
    }
    auto inputStage = build(ln->children[0]);
    return _limit
        ? std::move(inputStage)
        : std::make_unique<sbe::LimitSkipStage>(std::move(inputStage), ln->limit, boost::none);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildSkip(const QuerySolutionNode* root) {
    const auto sn = static_cast<const SkipNode*>(root);
    auto inputStage = build(sn->children[0]);
    return std::make_unique<sbe::LimitSkipStage>(std::move(inputStage), _limit, sn->skip);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildSort(const QuerySolutionNode* root) {
    using namespace std::literals;

    const auto sn = static_cast<const SortNode*>(root);
    auto sortPattern = SortPattern{sn->pattern, _cq.getExpCtx()};
    auto inputStage = build(sn->children[0]);
    std::vector<sbe::value::SlotId> orderBy;
    std::vector<sbe::value::SortDirection> direction;
    std::unordered_map<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>> projectMap;

    for (const auto& part : sortPattern) {
        uassert(ErrorCodes::InternalErrorNotSupported,
                "Sorting by expression not supported",
                !part.expression);
        uassert(ErrorCodes::InternalErrorNotSupported,
                "Sorting by dotted paths not supported",
                part.fieldPath && part.fieldPath->getPathLength() == 1);

        // slot holding the sort key
        auto sortFieldVar{_slotIdGenerator.generate()};
        orderBy.push_back(sortFieldVar);
        direction.push_back(part.isAscending ? sbe::value::SortDirection::Ascending
                                             : sbe::value::SortDirection::Descending);

        // Generate projection to get the value of the soft key. Ideally, this should be
        // tracked by a 'reference tracker' at higher level.
        auto fieldName = part.fieldPath->getFieldName(0);
        auto fieldNameSV = std::string_view{fieldName.rawData(), fieldName.size()};
        projectMap.emplace(
            sortFieldVar,
            sbe::makeE<sbe::EFunction>("getField"sv,
                                       sbe::makeEs(sbe::makeE<sbe::EVariable>(*_data.resultSlot),
                                                   sbe::makeE<sbe::EConstant>(fieldNameSV))));
    }

    inputStage = sbe::makeS<sbe::ProjectStage>(std::move(inputStage), std::move(projectMap));

    // Generate traversals to pick the min/max element from arrays.
    for (size_t idx = 0; idx < orderBy.size(); ++idx) {
        auto resultVar{_slotIdGenerator.generate()};
        auto innerVar{_slotIdGenerator.generate()};

        auto innerBranch = sbe::makeProjectStage(
            sbe::makeS<sbe::LimitSkipStage>(sbe::makeS<sbe::CoScanStage>(), 1, boost::none),
            innerVar,
            sbe::makeE<sbe::EVariable>(orderBy[idx]));

        auto op = direction[idx] == sbe::value::SortDirection::Ascending
            ? sbe::EPrimBinary::less
            : sbe::EPrimBinary::greater;
        auto minmax = sbe::makeE<sbe::EIf>(
            sbe::makeE<sbe::EPrimBinary>(
                op,
                sbe::makeE<sbe::EPrimBinary>(sbe::EPrimBinary::cmp3w,
                                             sbe::makeE<sbe::EVariable>(innerVar),
                                             sbe::makeE<sbe::EVariable>(resultVar)),
                sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::NumberInt64, 0)),
            sbe::makeE<sbe::EVariable>(innerVar),
            sbe::makeE<sbe::EVariable>(resultVar));

        inputStage = sbe::makeS<sbe::TraverseStage>(std::move(inputStage),
                                                    std::move(innerBranch),
                                                    orderBy[idx],
                                                    resultVar,
                                                    innerVar,
                                                    std::vector<sbe::value::SlotId>{},
                                                    std::move(minmax),
                                                    nullptr);
        orderBy[idx] = resultVar;
    }

    std::vector<sbe::value::SlotId> values;
    values.push_back(*_data.resultSlot);
    if (_data.recordIdSlot) {
        // Break ties with record id if awailable.
        orderBy.push_back(*_data.recordIdSlot);
        // This is arbitrary.
        direction.push_back(sbe::value::SortDirection::Ascending);
    }
    if (_data.oplogTsSlot) {
        values.push_back(*_data.oplogTsSlot);
    }
    return sbe::makeS<sbe::SortStage>(std::move(inputStage),
                                      orderBy,
                                      direction,
                                      values,
                                      sn->limit ? sn->limit
                                                : std::numeric_limits<std::size_t>::max());
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildSortKeyGeneraror(
    const QuerySolutionNode* root) {
    const auto kn = static_cast<const SortKeyGeneratorNode*>(root);
    // SBE does not use key generator, skip it.
    return build(kn->children[0]);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildProjectionSimple(
    const QuerySolutionNode* root) {
    using namespace std::literals;

    auto pn = static_cast<const ProjectionNodeSimple*>(root);
    auto inputStage = build(pn->children[0]);
    std::unordered_map<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>> projections;
    std::vector<sbe::value::SlotId> fieldSlots;

    for (const auto& field : pn->proj.getRequiredFields()) {
        fieldSlots.push_back(_slotIdGenerator.generate());
        projections.emplace(
            fieldSlots.back(),
            sbe::makeE<sbe::EFunction>("getField"sv,
                                       sbe::makeEs(sbe::makeE<sbe::EVariable>(*_data.resultSlot),
                                                   sbe::makeE<sbe::EConstant>(std::string_view{
                                                       field.c_str(), field.size()}))));
    }

    return sbe::makeS<sbe::MakeObjStage>(
        sbe::makeS<sbe::ProjectStage>(std::move(inputStage), std::move(projections)),
        *_data.resultSlot,
        boost::none,
        std::vector<std::string>{},
        pn->proj.getRequiredFields(),
        fieldSlots,
        true,
        false);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildProjectionDefault(
    const QuerySolutionNode* root) {
    using namespace std::literals;

    auto pn = static_cast<const ProjectionNodeDefault*>(root);
    auto inputStage = build(pn->children[0]);
    invariant(_data.resultSlot);
    auto [slot, stage] = generateProjection(
        &pn->proj, std::move(inputStage), &_slotIdGenerator, &_frameIdGenerator, *_data.resultSlot);
    _data.resultSlot = slot;
    return std::move(stage);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildOr(const QuerySolutionNode* root) {
    std::vector<std::unique_ptr<sbe::PlanStage>> inputStages;
    std::vector<std::vector<sbe::value::SlotId>> inputSlots;

    auto orn = static_cast<const OrNode*>(root);
    for (auto&& child : orn->children) {
        inputStages.push_back(build(child));
        invariant(_data.recordIdSlot);
        inputSlots.push_back({*_data.recordIdSlot});
    }

    _data.recordIdSlot = _slotIdGenerator.generate();
    std::vector<sbe::value::SlotId> outputSlots{*_data.recordIdSlot};
    auto stage = sbe::makeS<sbe::UnionStage>(std::move(inputStages), inputSlots, outputSlots);

    if (orn->dedup) {
        stage = sbe::makeS<sbe::HashAggStage>(
            std::move(stage),
            outputSlots,
            std::unordered_map<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>>{});
    }

    if (orn->filter) {
        stage = generateFilter(
            orn->filter.get(), std::move(stage), &_slotIdGenerator, *_data.resultSlot);
    }

    return stage;
}

// Returns a non-null pointer to the root of a plan tree, or a non-OK status if the PlanStage tree
// could not be constructed.
std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::build(const QuerySolutionNode* root) {
    static const std::unordered_map<StageType,
                                    std::function<std::unique_ptr<sbe::PlanStage>(
                                        SlotBasedStageBuilder&, const QuerySolutionNode* root)>>
        kStageBuilders = {
            {STAGE_COLLSCAN, std::mem_fn(&SlotBasedStageBuilder::buildCollScan)},
            {STAGE_IXSCAN, std::mem_fn(&SlotBasedStageBuilder::buildIndexScan)},
            {STAGE_FETCH, std::mem_fn(&SlotBasedStageBuilder::buildFetch)},
            {STAGE_LIMIT, std::mem_fn(&SlotBasedStageBuilder::buildLimit)},
            {STAGE_SKIP, std::mem_fn(&SlotBasedStageBuilder::buildSkip)},
            {STAGE_SORT_SIMPLE, std::mem_fn(&SlotBasedStageBuilder::buildSort)},
            {STAGE_SORT_DEFAULT, std::mem_fn(&SlotBasedStageBuilder::buildSort)},
            {STAGE_SORT_KEY_GENERATOR, std::mem_fn(&SlotBasedStageBuilder::buildSortKeyGeneraror)},
            {STAGE_PROJECTION_SIMPLE, std::mem_fn(&SlotBasedStageBuilder::buildProjectionSimple)},
            {STAGE_PROJECTION_DEFAULT, std::mem_fn(&SlotBasedStageBuilder::buildProjectionDefault)},
            {STAGE_OR, &SlotBasedStageBuilder::buildOr}};

    uassert(ErrorCodes::InternalErrorNotSupported,
            str::stream() << "Can't build exec tree for node: " << root->toString(),
            kStageBuilders.find(root->getType()) != kStageBuilders.end());

    return std::invoke(kStageBuilders.at(root->getType()), *this, root);
}
}  // namespace mongo::stage_builder
