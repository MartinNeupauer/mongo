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

namespace mongo {
namespace sbe {

class TraverseStage final : public PlanStage {
    const std::string _inField;
    const std::string _outField;
    const std::string _outFieldInner;
    const std::unique_ptr<EExpression> _fold;
    const std::unique_ptr<EExpression> _final;

    value::SlotAccessor* _inFieldAccessor{nullptr};
    std::unique_ptr<value::ViewOfValueAccessor> _correlatedAccessor;
    std::unique_ptr<value::OwnedValueAccessor> _outFieldOutputAccessor;
    value::SlotAccessor* _outFieldInputAccessor{nullptr};

    std::unique_ptr<vm::CodeFragment> _foldCode;
    std::unique_ptr<vm::CodeFragment> _finalCode;

    vm::ByteCode _bytecode;

    value::ArrayAccessor _inArrayAccessor;

    bool _compiled{false};
    bool _reOpenInner{false};

    void openInner(value::TypeTags tag, value::Value val);

public:
    TraverseStage(std::unique_ptr<PlanStage> outer,
                  std::unique_ptr<PlanStage> inner,
                  std::string_view inField,
                  std::string_view outField,
                  std::string_view outFieldInner,
                  std::unique_ptr<EExpression> foldExpr,
                  std::unique_ptr<EExpression> finalExpr);

    std::unique_ptr<PlanStage> clone() override;

    void prepare(CompileCtx& ctx) override;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, std::string_view field) override;
    void open(bool reOpen) override;
    PlanState getNext() override;
    void close() override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};
}  // namespace sbe
}  // namespace mongo