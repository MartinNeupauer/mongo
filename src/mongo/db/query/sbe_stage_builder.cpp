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
#include "mongo/db/exec/sbe/stages/exchange.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/hash_agg.h"
#include "mongo/db/exec/sbe/stages/ix_scan.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/makeobj.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/query/index_bounds_builder.h"
#include "mongo/db/query/sbe_stage_builder_filter.h"
#include "mongo/db/query/sbe_stage_builder_projection.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

namespace mongo::stage_builder {
namespace {
/**
 * Constructs start/stop key values from the given index 'bounds' and generates SlotId's to bind
 * these keys to.
 */
std::tuple<boost::optional<sbe::value::SlotId>,
           boost::optional<sbe::value::SlotId>,
           std::unique_ptr<KeyString::Value>,
           std::unique_ptr<KeyString::Value>>
makeLowAndHighKeysFromIndexBounds(const IndexBounds& bounds,
                                  bool forward,
                                  const IndexAccessMethod* accessMethod,
                                  sbe::value::SlotIdGenerator* slotIdGenerator) {
    auto startKey{bounds.startKey};
    auto endKey{bounds.endKey};
    auto startKeyInclusive{IndexBounds::isStartIncludedInBound(bounds.boundInclusion)};
    auto endKeyInclusive{IndexBounds::isEndIncludedInBound(bounds.boundInclusion)};

    if (bounds.isSimpleRange ||
        IndexBoundsBuilder::isSingleInterval(
            bounds, &startKey, &startKeyInclusive, &endKey, &endKeyInclusive)) {
        auto lowKey = std::make_unique<KeyString::Value>(
            IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(
                startKey,
                accessMethod->getSortedDataInterface()->getKeyStringVersion(),
                accessMethod->getSortedDataInterface()->getOrdering(),
                forward,
                startKeyInclusive));
        auto highKey = std::make_unique<KeyString::Value>(
            IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(
                endKey,
                accessMethod->getSortedDataInterface()->getKeyStringVersion(),
                accessMethod->getSortedDataInterface()->getOrdering(),
                forward,
                // Use the opposite rule as a normal seek because a forward scan should end after
                // the key if inclusive, and before if exclusive.
                forward != endKeyInclusive));

        return {slotIdGenerator->generate(),
                slotIdGenerator->generate(),
                std::move(lowKey),
                std::move(highKey)};

    } else {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  "Multi-interval index scans not supported in SBE");
    };
}
}  // namespace

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildCollScan(
    const QuerySolutionNode* root) {
    auto csn = static_cast<const CollectionScanNode*>(root);

    uassert(ErrorCodes::InternalErrorNotSupported,
            "Tailable collection scans are not supported in SBE",
            !csn->tailable);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Only forward collection scans are supported in SBE",
            csn->direction == 1);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with shouldTrackLatestOplogTimestamp are not supported "
            "in SBE",
            !csn->shouldTrackLatestOplogTimestamp);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with shouldWaitForOplogVisibility are not supported in SBE",
            !csn->shouldWaitForOplogVisibility);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with minTs are not supported in SBE",
            !csn->minTs);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with maxTs are not supported in SBE",
            !csn->maxTs);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with requestResumeToken are not supported in SBE",
            !csn->requestResumeToken);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with resumeAfterRecordId are not supported in SBE",
            !csn->resumeAfterRecordId);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Collection scans with stopApplyingFilterAfterFirstMatch are not supported "
            "in SBE",
            !csn->stopApplyingFilterAfterFirstMatch);

    _resultSlot = _slotIdGenerator->generate();
    _recordIdSlot = _slotIdGenerator->generate();
    size_t localDop = internalQueryDefaultDOP.load();
    std::unique_ptr<sbe::PlanStage> stage;
    if (localDop > 1) {
        stage = sbe::makeS<sbe::ParallelScanStage>(
            NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
            _resultSlot,
            _recordIdSlot,
            std::vector<std::string>{},
            std::vector<sbe::value::SlotId>{});
    } else {
        stage = sbe::makeS<sbe::ScanStage>(
            NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
            _resultSlot,
            _recordIdSlot,
            std::vector<std::string>{},
            std::vector<sbe::value::SlotId>{},
            boost::none);
    }

    if (csn->filter) {
        stage = generateFilter(
            csn->filter.get(), std::move(stage), _slotIdGenerator.get(), *_resultSlot);
    }

    if (localDop > 1) {
        std::vector<sbe::value::SlotId> fields;
        fields.push_back(*_resultSlot);
        fields.push_back(*_recordIdSlot);
        stage = sbe::makeS<sbe::ExchangeConsumer>(
            std::move(stage), localDop, fields, sbe::ExchangePolicy::roundrobin, nullptr, nullptr);
    }

    return stage;
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildIndexScan(
    const QuerySolutionNode* root) {
    auto ixn = static_cast<const IndexScanNode*>(root);

    uassert(ErrorCodes::InternalErrorNotSupported,
            "Only forward index scans are supported in SBE",
            ixn->direction == 1);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Index scans with key metadata are not supported in SBE",
            !ixn->addKeyMetadata);
    uassert(ErrorCodes::InternalErrorNotSupported,
            "Index scans with a filter are not supported in SBE",
            !ixn->filter);

    auto descriptor =
        _collection->getIndexCatalog()->findIndexByName(_opCtx, ixn->index.identifier.catalogName);
    auto [lowKeySlot, highKeySlot, lowKey, highKey] = makeLowAndHighKeysFromIndexBounds(
        ixn->bounds,
        ixn->direction == 1,
        _collection->getIndexCatalog()->getEntry(descriptor)->accessMethod(),
        _slotIdGenerator.get());

    // Scan the index in the range {'lowKeySlot', 'highKeySlot'} (subject to inclusive
    // or exclusive boundaries), and produce a single field recordIdSlot that can be
    // used to position into the collection.
    _recordIdSlot = _slotIdGenerator->generate();
    auto stage = sbe::makeS<sbe::IndexScanStage>(
        NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
        ixn->index.identifier.catalogName,
        boost::none,
        _recordIdSlot,
        std::vector<std::string>{},
        std::vector<sbe::value::SlotId>{},
        lowKeySlot,
        highKeySlot);
    if (lowKeySlot && highKeySlot) {
        // Construct a constant table scan to deliver a single row with two fields
        // 'lowKeySlot' and 'highKeySlot', representing seek boundaries, into the
        // index scan.
        auto projectKeysStage = sbe::makeProjectStage(
            sbe::makeS<sbe::LimitSkipStage>(sbe::makeS<sbe::CoScanStage>(), 1, boost::none),
            *lowKeySlot,
            sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::ksValue,
                                       sbe::value::bitcastFrom(lowKey.release())),
            *highKeySlot,
            sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::ksValue,
                                       sbe::value::bitcastFrom(highKey.release())));

        // Get the keys from the outer side (guaranteed to see exactly one row) and feed
        // them to the inner side.
        stage = sbe::makeS<sbe::LoopJoinStage>(
            std::move(projectKeysStage),
            std::move(stage),
            std::vector<sbe::value::SlotId>{},
            std::vector<sbe::value::SlotId>{*lowKeySlot, *highKeySlot},
            nullptr);
    } else {
        invariant(!lowKeySlot && !highKeySlot);
    }

    if (ixn->shouldDedup) {
        stage = sbe::makeS<sbe::HashAggStage>(
            std::move(stage),
            std::vector<sbe::value::SlotId>{*_recordIdSlot},
            std::unordered_map<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>>{});
    }

    return stage;
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildFetch(const QuerySolutionNode* root) {
    auto fn = static_cast<const FetchNode*>(root);
    auto inputStage = build(fn->children[0]);

    uassert(ErrorCodes::InternalError, "RecordId slot is not defined", _recordIdSlot);

    auto recordIdKeySlot = _recordIdSlot;
    _resultSlot = _slotIdGenerator->generate();
    _recordIdSlot = _slotIdGenerator->generate();

    // Scan the collection in the range [recordIdKeySlot, recordIdKeySlot).
    auto collScan = sbe::makeS<sbe::ScanStage>(
        NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
        _resultSlot,
        _recordIdSlot,
        std::vector<std::string>{},
        std::vector<sbe::value::SlotId>{},
        recordIdKeySlot);

    // Get the recordIdKeySlot from the outer side (e.g., IXSCAN) and feed it to the
    // inner side.
    auto stage = sbe::makeS<sbe::LoopJoinStage>(std::move(inputStage),
                                                std::move(collScan),
                                                std::vector<sbe::value::SlotId>{},
                                                std::vector<sbe::value::SlotId>{*recordIdKeySlot},
                                                nullptr);

    if (fn->filter) {
        stage = generateFilter(
            fn->filter.get(), std::move(stage), _slotIdGenerator.get(), *_resultSlot);
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
        auto sortFieldVar{_slotIdGenerator->generate()};
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
                                       sbe::makeEs(sbe::makeE<sbe::EVariable>(*_resultSlot),
                                                   sbe::makeE<sbe::EConstant>(fieldNameSV))));
    }

    inputStage = sbe::makeS<sbe::ProjectStage>(std::move(inputStage), std::move(projectMap));

    // Generate traversals to pick the min/max element from arrays.
    for (size_t idx = 0; idx < orderBy.size(); ++idx) {
        auto resultVar{_slotIdGenerator->generate()};
        auto innerVar{_slotIdGenerator->generate()};

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
                                                    std::move(minmax),
                                                    nullptr);
        orderBy[idx] = resultVar;
    }

    std::vector<sbe::value::SlotId> values;
    values.push_back(*_resultSlot);
    if (_recordIdSlot) {
        // Break ties with record id if awailable.
        orderBy.push_back(*_recordIdSlot);
        // This is arbitrary.
        direction.push_back(sbe::value::SortDirection::Ascending);
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
        fieldSlots.push_back(_slotIdGenerator->generate());
        projections.emplace(
            fieldSlots.back(),
            sbe::makeE<sbe::EFunction>("getField"sv,
                                       sbe::makeEs(sbe::makeE<sbe::EVariable>(*_resultSlot),
                                                   sbe::makeE<sbe::EConstant>(std::string_view{
                                                       field.c_str(), field.size()}))));
    }

    return sbe::makeS<sbe::MakeObjStage>(
        sbe::makeS<sbe::ProjectStage>(std::move(inputStage), std::move(projections)),
        *_resultSlot,
        boost::none,
        std::vector<std::string>{},
        pn->proj.getRequiredFields(),
        fieldSlots);
}

std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::buildProjectionDefault(
    const QuerySolutionNode* root) {
    using namespace std::literals;

    auto pn = static_cast<const ProjectionNodeDefault*>(root);
    auto inputStage = build(pn->children[0]);
    invariant(_resultSlot);
    auto [slot, stage] =
        generateProjection(&pn->proj, std::move(inputStage), _slotIdGenerator.get(), *_resultSlot);
    _resultSlot = slot;
    return std::move(stage);
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
            {STAGE_PROJECTION_DEFAULT,
             std::mem_fn(&SlotBasedStageBuilder::buildProjectionDefault)}};

    uassert(ErrorCodes::InternalErrorNotSupported,
            str::stream() << "Can't build exec tree for node: " << root->toString(),
            kStageBuilders.find(root->getType()) != kStageBuilders.end());

    auto stage = std::invoke(kStageBuilders.at(root->getType()), *this, root);
    if (root == _solution.root.get()) {
        uassert(ErrorCodes::InternalError, "Result slot is not defined in SBE plan", _resultSlot);

        stage = _recordIdSlot ? sbe::makeProjectStage(std::move(stage),
                                                      sbe::value::SystemSlots::kResultSlot,
                                                      sbe::makeE<sbe::EVariable>(*_resultSlot),
                                                      sbe::value::SystemSlots::kRecordIdSlot,
                                                      sbe::makeE<sbe::EVariable>(*_recordIdSlot))
                              : sbe::makeProjectStage(std::move(stage),
                                                      sbe::value::SystemSlots::kResultSlot,
                                                      sbe::makeE<sbe::EVariable>(*_resultSlot));
    }

    return stage;
}
}  // namespace mongo::stage_builder
