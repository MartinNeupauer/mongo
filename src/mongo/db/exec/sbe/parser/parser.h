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
#include "mongo/db/exec/sbe/parser/peglib.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/values/slot_id_generator.h"
#include "mongo/db/query/sbe_stage_builder.h"

namespace mongo {
namespace sbe {
struct ParsedQueryTree {
    std::string identifier;
    std::string rename;
    std::vector<std::string> identifiers;
    std::vector<std::string> renames;

    std::unique_ptr<PlanStage> stage;
    std::unique_ptr<EExpression> expr;

    std::unordered_map<std::string, std::unique_ptr<EExpression>> projects;
};

using AstQuery = peg::AstBase<ParsedQueryTree>;

class Parser {
    peg::parser _parser;
    OperationContext* _opCtx{nullptr};
    std::string _defaultDb;
    std::unordered_map<std::string, value::SlotId> _symbolsLookupTable;
    std::unique_ptr<value::SlotIdGenerator> _slotIdGenerator{value::makeDefaultSlotIdGenerator()};

    boost::optional<value::SlotId> lookupSlot(const std::string& name) {
        if (name.empty()) {
            return boost::none;
        } else if (name == "$$RESULT") {
            return value::SystemSlots::kResultSlot;
        } else if (name == "$$RID") {
            return value::SystemSlots::kRecordIdSlot;
        } else if (_symbolsLookupTable.find(name) == _symbolsLookupTable.end()) {
            _symbolsLookupTable[name] = _slotIdGenerator->generate();
            std::cout << "mapping " << name << " to " << _symbolsLookupTable[name] << std::endl;
        }
        return _symbolsLookupTable[name];
    }

    value::SlotId lookupSlotStrict(const std::string& name) {
        auto slot = lookupSlot(name);
        uassert(ErrorCodes::InternalError,
                str::stream() << "Unable lookup SlotId for [" << name << "]",
                slot);
        return *slot;
    }

    std::vector<value::SlotId> lookupSlots(const std::vector<std::string>& names) {
        std::vector<value::SlotId> result;
        std::transform(names.begin(),
                       names.end(),
                       std::back_inserter(result),
                       [this](const auto& name) { return lookupSlotStrict(name); });
        return result;
    }

    template <typename T>
    std::unordered_map<value::SlotId, T> lookupSlots(std::unordered_map<std::string, T> map) {
        std::unordered_map<value::SlotId, T> result;
        for (auto&& [k, v] : map) {
            result[lookupSlotStrict(k)] = std::move(v);
        }
        return result;
    }

    void walkChildren(AstQuery& ast);
    void walkIdent(AstQuery& ast);
    void walkIdentList(AstQuery& ast);
    void walkIdentWithRename(AstQuery& ast);
    void walkIdentListWithRename(AstQuery& ast);

    void walkProjectList(AstQuery& ast);
    void walkAssign(AstQuery& ast);
    void walkExpr(AstQuery& ast);
    void walkEqopExpr(AstQuery& ast);
    void walkRelopExpr(AstQuery& ast);
    void walkAddExpr(AstQuery& ast);
    void walkMulExpr(AstQuery& ast);
    void walkPrimaryExpr(AstQuery& ast);
    void walkIfExpr(AstQuery& ast);
    void walkFunCall(AstQuery& ast);
    void walkUnionBranch(AstQuery& ast);

    void walkScan(AstQuery& ast);
    void walkParallelScan(AstQuery& ast);
    void walkSeek(AstQuery& ast);
    void walkIndexScan(AstQuery& ast);
    void walkIndexSeek(AstQuery& ast);
    void walkProject(AstQuery& ast);
    void walkFilter(AstQuery& ast);
    void walkCFilter(AstQuery& ast);
    void walkSort(AstQuery& ast);
    void walkUnion(AstQuery& ast);
    void walkUnwind(AstQuery& ast);
    void walkMkObj(AstQuery& ast);
    void walkGroup(AstQuery& ast);
    void walkHashJoin(AstQuery& ast);
    void walkNLJoin(AstQuery& ast);
    void walkLimit(AstQuery& ast);
    void walkSkip(AstQuery& ast);
    void walkCoScan(AstQuery& ast);
    void walkTraverse(AstQuery& ast);
    void walkExchange(AstQuery& ast);

    void walk(AstQuery& ast);

public:
    Parser();
    std::unique_ptr<PlanStage> parse(OperationContext* opCtx,
                                     std::string_view defaultDb,
                                     std::string_view line);
};
}  // namespace sbe
}  // namespace mongo
