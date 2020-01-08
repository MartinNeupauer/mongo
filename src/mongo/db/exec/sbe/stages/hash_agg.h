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
#include "mongo/stdx/unordered_map.h"

#include <map>
#include <unordered_map>
#include <unordered_set>

namespace mongo {
namespace sbe {
class HashAggStage final : public PlanStage {
    using TableType = stdx::
        unordered_map<value::MaterializedRow, value::MaterializedRow, value::MaterializedRowHasher>;

    using HashKeyAccessor = value::MaterializedRowKeyAccessor<TableType::iterator>;
    using HashAggAccessor = value::MaterializedRowValueAccessor<TableType::iterator>;

    const std::vector<value::SlotId> _gbs;
    const std::unordered_map<value::SlotId, std::unique_ptr<EExpression>> _aggs;

    // now that _gbs is saved vector<value::Accessor*> should be enough
    std::map<value::SlotId, value::SlotAccessor*, std::less<>> _outAccessors;
    std::map<value::SlotId, value::SlotAccessor*, std::less<>> _inKeyAccessors;
    std::vector<std::unique_ptr<HashKeyAccessor>> _outKeyAccessors;

    std::vector<std::unique_ptr<HashAggAccessor>> _outAggAccessors;
    std::vector<std::unique_ptr<vm::CodeFragment>> _aggCodes;

    TableType _ht;
    TableType::iterator _htIt;

    vm::ByteCode _bytecode;

    bool _compiled{false};

public:
    HashAggStage(std::unique_ptr<PlanStage> input,
                 const std::vector<value::SlotId>& gbs,
                 std::unordered_map<value::SlotId, std::unique_ptr<EExpression>> aggs);

    std::unique_ptr<PlanStage> clone() final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::vector<DebugPrinter::Block> debugPrint() final;
};
}  // namespace sbe
}  // namespace mongo
