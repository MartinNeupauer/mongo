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
std::unordered_map<std::type_index, ABTBuilder::BuilderFnType> ABTBuilder::kStageBuilders;

MONGO_INITIALIZER(RegisterABTBuilders)(InitializerContext*) {

    ABTBuilder::registerBuilder<DocumentSourceUnwind>(std::mem_fn(&ABTBuilder::buildUnwind));
    ABTBuilder::registerBuilder<DocumentSourceGroup>(std::mem_fn(&ABTBuilder::buildGroup));

    return Status::OK();
}

DSABT ABTBuilder::buildUnwind(const DocumentSource* root) {
    using namespace sbe::abt;

    const auto un = static_cast<const DocumentSourceUnwind*>(root);
    auto input = build(un->getSource());

    auto rowsetTypeId = _rowsetIdGen.generate();
    auto rowsetVarId = _varIdGen.generate();
    auto docVarId = _varIdGen.generate();
    auto inputToUnwind = _varIdGen.generate();
    auto outputFromUnwind = _varIdGen.generate();

    // Project out the unwind field
    auto& path = un->unwindPath();
    auto abtPath = make<PathIdentity>();
    for (size_t idx = path.getPathLength(); idx-- > 0;) {
        abtPath = make<PathGet>(path.getFieldName(idx).toString(), std::move(abtPath));
    }
    abtPath = make<EvalPath>(std::move(abtPath), var(input.doc));

    auto x = _varIdGen.generate();
    auto abtPathOut = make<PathLambda>(lam(x,
                                           _if(op(BinaryOp::logicAnd,
                                                  fun("exists", var(inputToUnwind)),
                                                  fun("isArray", var(inputToUnwind))),
                                               var(outputFromUnwind),
                                               var(x))));

    abtPathOut = make<PathField>(path.back().toString(), std::move(abtPathOut));
    for (size_t idx = path.getPathLength() - 1; idx-- > 0;) {
        abtPathOut = make<PathTraverse>(std::move(abtPathOut));
        abtPathOut = make<PathField>(path.getFieldName(idx).toString(), std::move(abtPathOut));
    }
    abtPathOut = make<EvalPath>(std::move(abtPathOut), var(input.doc));

    auto body = makeBinder(inputToUnwind,
                           fence(std::move(abtPath)),
                           rowsetVarId,
                           fence(fdep(rowsetType(rowsetTypeId), var(input.rowset))),
                           outputFromUnwind,
                           fence(make<ConstantMagic>(kVariantType)),
                           docVarId,
                           std::move(abtPathOut));

    return {make<Unwind>(un->preserveNullAndEmptyArrays(),
                         rowsetVarId,
                         inputToUnwind,
                         outputFromUnwind,
                         std::move(body),
                         makeSeq(std::move(input.op))),
            rowsetVarId,
            docVarId};
}
DSABT ABTBuilder::buildGroup(const DocumentSource* root) {
    using namespace sbe::abt;

    const auto gb = static_cast<const DocumentSourceGroup*>(root);
    auto input = build(gb->getSource());

    std::vector<VarId> gbs;
    std::vector<VarId> ids;
    std::vector<ABT> binds;

    // Project out group by fields/columns.
    for (size_t idx = 0; idx < gb->getIdExpressions().size(); ++idx) {
        auto fieldExpr = gb->getIdExpressions()[idx].get();
        auto fieldPathExpr = dynamic_cast<ExpressionFieldPath*>(fieldExpr);
        uassert(ErrorCodes::NotImplemented, "not yet implemented", fieldPathExpr);

        auto path = fieldPathExpr->getFieldPathWithoutCurrentPrefix();

        auto abtPath = make<PathIdentity>();
        abtPath = make<PathGet>(path.back().toString(), std::move(abtPath));
        for (size_t idx = path.getPathLength() - 1; idx-- > 0;) {
            abtPath = make<PathTraverse>(std::move(abtPath));
            abtPath = make<PathGet>(path.getFieldName(idx).toString(), std::move(abtPath));
        }
        abtPath = fence(make<EvalPath>(std::move(abtPath), var(input.doc)));

        auto gbVar = _varIdGen.generate();
        gbs.push_back(gbVar);
        ids.push_back(gbVar);
        binds.emplace_back(std::move(abtPath));
    }
    // Create agg expressions
    for (size_t idx = 0; idx < gb->getAccumulatedFields().size(); ++idx) {
        auto acc = gb->getAccumulatedFields()[idx].makeAccumulator();
    }

    auto rowsetTypeId = _rowsetIdGen.generate();
    auto rowsetVarId = _varIdGen.generate();
    ids.push_back(rowsetVarId);
    binds.emplace_back(fence(fdep(rowsetType(rowsetTypeId), var(input.rowset))));

    // Output of the group operation
    auto docVarId = _varIdGen.generate();
    if (gb->getIdFieldNames().empty()) {
        auto abtPathOut = make<PathConstant>(var(gbs[0]));
        abtPathOut = make<PathField>("_id", std::move(abtPathOut));
        auto [tag, val] = sbe::value::makeNewObject();
        abtPathOut = make<EvalPath>(std::move(abtPathOut), makeConst(tag, val));
        ids.push_back(docVarId);
        binds.emplace_back(std::move(abtPathOut));
    } else {
        auto abtPathOut = make<Blackhole>();
        for (size_t idx = 0; idx < gb->getIdFieldNames().size(); ++idx) {
            auto abt = make<PathConstant>(var(gbs[idx]));
            abt = make<PathField>(gb->getIdFieldNames()[idx], std::move(abt));
            if (idx == 0) {
                abtPathOut = std::move(abt);
            } else {

                abtPathOut = make<PathCompose>(std::move(abt), std::move(abtPathOut));
            }
        }
        abtPathOut = make<PathField>("_id", std::move(abtPathOut));
        auto [tag, val] = sbe::value::makeNewObject();
        abtPathOut = make<EvalPath>(std::move(abtPathOut), makeConst(tag, val));
        ids.push_back(docVarId);
        binds.emplace_back(std::move(abtPathOut));
    }

    return {make<Group>(rowsetVarId,
                        gbs,
                        make<ValueBinder>(std::move(ids), std::move(binds)),
                        makeSeq(std::move(input.op))),
            rowsetVarId,
            docVarId};
}
DSABT ABTBuilder::build(const DocumentSource* root) {
    uassert(ErrorCodes::InternalErrorNotSupported,
            str::stream() << "Can't build exec tree for node: " << root->getSourceName(),
            kStageBuilders.find(typeid(*root)) != kStageBuilders.end());

    return std::invoke(kStageBuilders.at(typeid(*root)), *this, root);
}

DSABT ABTBuilder::build(const Pipeline* pipeline) {
    auto stage = build(pipeline->getSources().back().get());

    // fence the output so it is not optimized away as it is not internally referenced
    auto& body = stage.op.cast<sbe::abt::OpSyntaxSort>()->body();
    auto binder = body.cast<sbe::abt::ValueBinder>();
    auto idx = binder->index(stage.doc);
    binder->binds()[idx] = sbe::abt::fence(std::move(binder->binds()[idx]));

    return stage;
}

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

    sbe::value::SlotVector gbs;
    sbe::value::SlotVector aggOut;
    sbe::value::SlotMap<std::unique_ptr<sbe::EExpression>> aggs;

    // Project out group by fields/columns.
    for (size_t idx = 0; idx < gb->getIdExpressions().size(); ++idx) {
        auto fieldExpr = gb->getIdExpressions()[idx].get();
        auto [slot, expr, stage] = generateExpression(
            fieldExpr, std::move(inputStage), &_slotIdGenerator, &_frameIdGenerator, *_resultSlot);

        inputStage = sbe::makeProjectStage(std::move(stage), slot, std::move(expr));
        gbs.push_back(slot);
    }

    // Create agg expressions
    for (size_t idx = 0; idx < gb->getAccumulatedFields().size(); ++idx) {
        auto acc = gb->getAccumulatedFields()[idx].makeAccumulator();
        if (acc->getOpName() == "$sum"_sd) {
            auto fieldExpr = gb->getAccumulatedFields()[idx].expr.argument.get();
            auto [slot, expr, stage] = generateExpression(fieldExpr,
                                                          std::move(inputStage),
                                                          &_slotIdGenerator,
                                                          &_frameIdGenerator,
                                                          *_resultSlot);

            inputStage = sbe::makeProjectStage(std::move(stage), slot, std::move(expr));

            auto slotOut = _slotIdGenerator.generate();
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
        resultIdSlot = _slotIdGenerator.generate();
        inputStage = sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                                   resultIdSlot,
                                                   boost::none,
                                                   std::vector<std::string>{},
                                                   gb->getIdFieldNames(),
                                                   std::move(gbs),
                                                   true,
                                                   false);
    }
    // Construct the result.
    _resultSlot = _slotIdGenerator.generate();
    std::vector<std::string> fieldsOut;
    sbe::value::SlotVector slotsOut;
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
                                               std::move(fieldsOut),
                                               std::move(slotsOut),
                                               true,
                                               false);

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
        &projection, std::move(inputStage), &_slotIdGenerator, &_frameIdGenerator, *_resultSlot);
    _resultSlot = slot;

    return std::move(stage);
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildMatch(
    const DocumentSource* root) {
    const auto fn = static_cast<const DocumentSourceMatch*>(root);
    auto inputStage = build(fn->getSource());

    auto stage = generateFilter(
        fn->getMatchExpression(), std::move(inputStage), &_slotIdGenerator, *_resultSlot);

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
    /*uassert(ErrorCodes::InternalErrorNotSupported,
            str::stream() << "Dotted paths in unwind not supported: " << path.fullPath(),
            path.getPathLength() == 1);
    */
    auto slot = _slotIdGenerator.generate();
    auto getFieldFn = sbe::makeE<sbe::EFunction>(
        "getField"sv,
        sbe::makeEs(sbe::makeE<sbe::EVariable>(*_resultSlot),
                    sbe::makeE<sbe::EConstant>(path.getSubpath(0).toString())));

    inputStage = sbe::makeProjectStage(std::move(inputStage), slot, std::move(getFieldFn));

    // Create the unwind
    auto indexSlot = _slotIdGenerator.generate();
    auto outSlot = _slotIdGenerator.generate();
    inputStage = sbe::makeS<sbe::UnwindStage>(
        std::move(inputStage), slot, outSlot, indexSlot, un->preserveNullAndEmptyArrays());

    // Construct the result
    auto oldResult = _resultSlot;
    _resultSlot = _slotIdGenerator.generate();
    std::vector<std::string> fieldsOut;
    sbe::value::SlotVector slotsOut;
    fieldsOut.push_back(path.fullPath());
    slotsOut.push_back(outSlot);


    inputStage = sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                               *_resultSlot,
                                               oldResult,
                                               std::vector<std::string>{},
                                               std::move(fieldsOut),
                                               std::move(slotsOut),
                                               true,
                                               false);
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

    return stage;
}
}  // namespace mongo::stage_builder
