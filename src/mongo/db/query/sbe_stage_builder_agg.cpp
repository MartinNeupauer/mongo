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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

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
#include "mongo/db/exec/sbe/stages/unwind.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "mongo/db/query/projection_parser.h"
#include "mongo/db/query/sbe_stage_builder_agg.h"
#include "mongo/db/query/sbe_stage_builder_expression.h"
#include "mongo/db/query/sbe_stage_builder_filter.h"
#include "mongo/db/query/sbe_stage_builder_projection.h"

namespace mongo::stage_builder {
std::unordered_map<std::type_index, DocumentSourceSlotBasedStageBuilder::BuilderFnType>
    DocumentSourceSlotBasedStageBuilder::kStageBuilders;

MONGO_INITIALIZER(RegisterDocumentSourceBuilders)(InitializerContext*) {
    DocumentSourceSlotBasedStageBuilder::registerBuilder<DocumentSourceGroup>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildGroup));

    DocumentSourceSlotBasedStageBuilder::registerBuilder<DocumentSourceLimit>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildLimit));

    DocumentSourceSlotBasedStageBuilder::registerBuilder<
        DocumentSourceSingleDocumentTransformation>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildTransform));

    DocumentSourceSlotBasedStageBuilder::registerBuilder<DocumentSourceMatch>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildMatch));

    DocumentSourceSlotBasedStageBuilder::registerBuilder<DocumentSourceSort>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildSort));

    DocumentSourceSlotBasedStageBuilder::registerBuilder<DocumentSourceUnwind>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildUnwind));

    return Status::OK();
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildGroup(
    const DocumentSource* root) {
    using namespace std::literals;
    const auto gb = static_cast<const DocumentSourceGroup*>(root);
    auto inputStage = build(gb->getSource());

    std::vector<sbe::value::SlotId> gbs;
    std::vector<sbe::value::SlotId> aggOut;
    std::unordered_map<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>> aggs;

    // Project out group by fields/columns.
    for (size_t idx = 0; idx < gb->getIdExpressions().size(); ++idx) {
        auto fieldExpr = gb->getIdExpressions()[idx].get();
        auto [slot, expr, stage] = generateExpression(
            fieldExpr, std::move(inputStage), _slotIdGenerator.get(), *_resultSlot);

        inputStage = sbe::makeProjectStage(std::move(stage), slot, std::move(expr));
        gbs.push_back(slot);
    }

    // Create agg expressions
    for (size_t idx = 0; idx < gb->getAccumulatedFields().size(); ++idx) {
        auto acc = gb->getAccumulatedFields()[idx].makeAccumulator();
        if (acc->getOpName() == "$sum") {
            auto fieldExpr = gb->getAccumulatedFields()[idx].expr.argument.get();
            auto [slot, expr, stage] = generateExpression(
                fieldExpr, std::move(inputStage), _slotIdGenerator.get(), *_resultSlot);

            inputStage = sbe::makeProjectStage(std::move(stage), slot, std::move(expr));

            auto slotOut = _slotIdGenerator->generate();
            aggOut.push_back(slotOut);
            aggs[slotOut] =
                sbe::makeE<sbe::EFunction>("sum"sv, sbe::makeEs(sbe::makeE<sbe::EVariable>(slot)));
        } else {
            uasserted(ErrorCodes::InternalErrorNotSupported,
                      str::stream() << "unsupported accumulator: " << acc->getOpName());
        }
    }
    // Create the group by stage.
    inputStage = sbe::makeS<sbe::HashAggStage>(std::move(inputStage), gbs, std::move(aggs));

    sbe::value::SlotId resultIdSlot;
    // Construct _id.
    if (gb->getIdFieldNames().empty()) {
        invariant(gbs.size() == 1);
        resultIdSlot = gbs[0];
    } else {
        resultIdSlot = _slotIdGenerator->generate();
        inputStage = sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                                   resultIdSlot,
                                                   boost::none,
                                                   std::vector<std::string>{},
                                                   gb->getIdFieldNames(),
                                                   gbs);
    }
    // Construct the result.
    _resultSlot = _slotIdGenerator->generate();
    std::vector<std::string> fieldsOut;
    std::vector<sbe::value::SlotId> slotsOut;
    fieldsOut.push_back("_id");
    slotsOut.push_back(resultIdSlot);
    for (size_t idx = 0; idx < aggOut.size(); ++idx) {
        fieldsOut.push_back(gb->getAccumulatedFields()[idx].fieldName);
        slotsOut.push_back(aggOut[idx]);
    }
    inputStage = sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                               *_resultSlot,
                                               boost::none,
                                               std::vector<std::string>{},
                                               fieldsOut,
                                               slotsOut);

    return inputStage;
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildLimit(
    const DocumentSource* root) {
    const auto ln = static_cast<const DocumentSourceLimit*>(root);
    auto inputStage = build(ln->getSource());

    return std::make_unique<sbe::LimitSkipStage>(
        std::move(inputStage), ln->getLimit(), boost::none);
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildTransform(
    const DocumentSource* root) {
    const auto prj = static_cast<const DocumentSourceSingleDocumentTransformation*>(root);
    auto inputStage = build(prj->getSource());

    // We have to come up with more sane way to create projections.
    auto trn = prj->getTransformer().serializeTransformation(boost::none);

    auto policies = ProjectionPolicies::aggregateProjectionPolicies();
    auto projection = projection_ast::parse(prj->getContext(), trn.toBson(), policies);

    auto [slot, stage] = generateProjection(
        &projection, std::move(inputStage), _slotIdGenerator.get(), *_resultSlot);
    _resultSlot = slot;

    return std::move(stage);
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildMatch(
    const DocumentSource* root) {
    const auto fn = static_cast<const DocumentSourceMatch*>(root);
    auto inputStage = build(fn->getSource());

    auto stage = generateFilter(
        fn->getMatchExpression(), std::move(inputStage), _slotIdGenerator.get(), *_resultSlot);

    return stage;
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildSort(
    const DocumentSource* root) {
    const auto sn = static_cast<const DocumentSourceSort*>(root);
    auto inputStage = build(sn->getSource());

    return inputStage;
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildUnwind(
    const DocumentSource* root) {
    using namespace std::literals;
    const auto un = static_cast<const DocumentSourceUnwind*>(root);
    auto inputStage = build(un->getSource());

    // Project out the unwind field
    auto& path = un->unwindPath();
    uassert(ErrorCodes::InternalErrorNotSupported,
            str::stream() << "Dotted paths in unwind not supported: " << path.fullPath(),
            path.getPathLength() == 1);

    auto slot = _slotIdGenerator->generate();
    auto getFieldFn =
        sbe::makeE<sbe::EFunction>("getField"sv,
                                   sbe::makeEs(sbe::makeE<sbe::EVariable>(*_resultSlot),
                                               sbe::makeE<sbe::EConstant>(path.fullPath())));

    inputStage = sbe::makeProjectStage(std::move(inputStage), slot, std::move(getFieldFn));

    // Create the unwind
    auto indexSlot = _slotIdGenerator->generate();
    auto outSlot = _slotIdGenerator->generate();
    inputStage = sbe::makeS<sbe::UnwindStage>(std::move(inputStage), slot, outSlot, indexSlot);

    // Construct the result
    auto oldResult = _resultSlot;
    _resultSlot = _slotIdGenerator->generate();
    std::vector<std::string> fieldsOut;
    std::vector<sbe::value::SlotId> slotsOut;
    fieldsOut.push_back(path.fullPath());
    slotsOut.push_back(outSlot);


    inputStage = sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                               *_resultSlot,
                                               oldResult,
                                               std::vector<std::string>{},
                                               fieldsOut,
                                               slotsOut);
    return inputStage;
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::build(
    const DocumentSource* root) {
    uassert(ErrorCodes::InternalErrorNotSupported,
            str::stream() << "Can't build exec tree for node: " << root->getSourceName(),
            kStageBuilders.find(typeid(*root)) != kStageBuilders.end());

    auto stage = std::invoke(kStageBuilders.at(typeid(*root)), *this, root);
    return stage;
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::build(
    const Pipeline* pipeline) {
    auto stage = build(pipeline->getSources().back().get());

    uassert(ErrorCodes::InternalError, "Result slot is not defined in SBE plan", _resultSlot);

    stage = _recordIdSlot ? sbe::makeProjectStage(std::move(stage),
                                                  sbe::value::SystemSlots::kResultSlot,
                                                  sbe::makeE<sbe::EVariable>(*_resultSlot),
                                                  sbe::value::SystemSlots::kRecordIdSlot,
                                                  sbe::makeE<sbe::EVariable>(*_recordIdSlot))
                          : sbe::makeProjectStage(std::move(stage),
                                                  sbe::value::SystemSlots::kResultSlot,
                                                  sbe::makeE<sbe::EVariable>(*_resultSlot));
    return stage;
}
}  // namespace mongo::stage_builder
