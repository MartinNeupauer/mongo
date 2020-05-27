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
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/vm/vm.h"

namespace mongo::sbe {
class TraverseStage final : public PlanStage {
public:
    TraverseStage(std::unique_ptr<PlanStage> outer,
                  std::unique_ptr<PlanStage> inner,
                  value::SlotId inField,
                  value::SlotId outField,
                  value::SlotId outFieldInner,
                  value::SlotVector outerCorrelated,
                  std::unique_ptr<EExpression> foldExpr,
                  std::unique_ptr<EExpression> finalExpr,
                  boost::optional<size_t> nestedArraysDepth = boost::none);

    std::unique_ptr<PlanStage> clone() const final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::unique_ptr<PlanStageStats> getStats() const final;
    const SpecificStats* getSpecificStats() const final;
    std::vector<DebugPrinter::Block> debugPrint() const final;

private:
    void openInner(value::TypeTags tag, value::Value val);
    bool traverse(value::SlotAccessor* inFieldAccessor,
                  value::OwnedValueAccessor* outFieldOutputAccessor,
                  size_t level);

    const value::SlotId _inField;
    const value::SlotId _outField;
    const value::SlotId _outFieldInner;
    const value::SlotVector _correlatedSlots;
    const std::unique_ptr<EExpression> _fold;
    const std::unique_ptr<EExpression> _final;
    const boost::optional<size_t> _nestedArraysDepth;

    value::SlotAccessor* _inFieldAccessor{nullptr};
    value::ViewOfValueAccessor _correlatedAccessor;
    value::OwnedValueAccessor _outFieldOutputAccessor;
    value::SlotAccessor* _outFieldInputAccessor{nullptr};

    std::unique_ptr<vm::CodeFragment> _foldCode;
    std::unique_ptr<vm::CodeFragment> _finalCode;

    vm::ByteCode _bytecode;

    bool _compiled{false};
    bool _reOpenInner{false};
};
}  // namespace mongo::sbe
