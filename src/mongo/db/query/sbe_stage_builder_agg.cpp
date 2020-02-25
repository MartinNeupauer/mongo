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
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/query/sbe_stage_builder_agg.h"

namespace mongo::stage_builder {
std::unordered_map<std::type_index, DocumentSourceSlotBasedStageBuilder::BuilderFnType>
    DocumentSourceSlotBasedStageBuilder::kStageBuilders;

MONGO_INITIALIZER(RegisterDocumentSourceBuilders)(InitializerContext*) {
    DocumentSourceSlotBasedStageBuilder::registerBuilder<DocumentSourceGroup>(
        std::mem_fn(&DocumentSourceSlotBasedStageBuilder::buildGroup));

    return Status::OK();
}

std::unique_ptr<sbe::PlanStage> DocumentSourceSlotBasedStageBuilder::buildGroup(
    const DocumentSource* root) {
    const auto gb = static_cast<const DocumentSourceGroup*>(root);
    auto inputStage = build(gb->getSource());

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
