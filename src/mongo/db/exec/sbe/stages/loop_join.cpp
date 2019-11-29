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

#include "mongo/db/exec/sbe/stages/loop_join.h"

namespace mongo {
namespace sbe {
LoopJoinStage::LoopJoinStage(std::unique_ptr<PlanStage> outer, std::unique_ptr<PlanStage> inner) {
    _children.emplace_back(std::move(outer));
    _children.emplace_back(std::move(inner));
}
std::unique_ptr<PlanStage> LoopJoinStage::clone() {
    return std::make_unique<LoopJoinStage>(_children[0]->clone(), _children[1]->clone());
}
void LoopJoinStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);
    _children[1]->prepare(ctx);
}
value::SlotAccessor* LoopJoinStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    return nullptr;
}
void LoopJoinStage::open(bool reOpen) {}
PlanState LoopJoinStage::getNext() {
    return PlanState::IS_EOF;
}
void LoopJoinStage::close() {}
std::vector<DebugPrinter::Block> LoopJoinStage::debugPrint() {
    return std::vector<DebugPrinter::Block>();
}
}  // namespace sbe
}  // namespace mongo