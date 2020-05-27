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
#include "mongo/db/pipeline/document_source.h"

namespace mongo::stage_builder {
class DocumentSourceSlotBasedStageBuilder {
    using BuilderFnType = std::function<std::unique_ptr<sbe::PlanStage>(
        DocumentSourceSlotBasedStageBuilder&, const DocumentSource* root)>;
    static std::unordered_map<std::type_index, DocumentSourceSlotBasedStageBuilder::BuilderFnType>
        kStageBuilders;

    sbe::value::SlotIdGenerator _slotIdGenerator;
    sbe::value::FrameIdGenerator _frameIdGenerator;
    boost::optional<sbe::value::SlotId> _recordIdSlot;
    boost::optional<sbe::value::SlotId> _resultSlot;

public:
    std::unique_ptr<sbe::PlanStage> build(const DocumentSource* root);
    std::unique_ptr<sbe::PlanStage> build(const Pipeline* pipeline);

    std::unique_ptr<sbe::PlanStage> buildGroup(const DocumentSource* root);
    std::unique_ptr<sbe::PlanStage> buildLimit(const DocumentSource* root);
    std::unique_ptr<sbe::PlanStage> buildTransform(const DocumentSource* root);
    std::unique_ptr<sbe::PlanStage> buildMatch(const DocumentSource* root);
    std::unique_ptr<sbe::PlanStage> buildSort(const DocumentSource* root);
    std::unique_ptr<sbe::PlanStage> buildUnwind(const DocumentSource* root);


    std::unique_ptr<sbe::PlanStage> buildBSONScan(const DocumentSource* root);

    template <typename T>
    static void registerBuilder(BuilderFnType f) {
        kStageBuilders[typeid(T)] = f;
    }
};
}  // namespace mongo::stage_builder
