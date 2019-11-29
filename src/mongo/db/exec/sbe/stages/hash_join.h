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

#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/vm/vm.h"

#include <map>
#include <unordered_map>
#include <vector>

namespace mongo {
namespace sbe {
class HashJoinStage final : public PlanStage {
    using TableType = std::unordered_multimap<value::MaterializedRow,
                                              value::MaterializedRow,
                                              value::MaterializedRowHasher>;

    using HashKeyAccessor = value::MaterializedRowKeyAccessor<TableType::iterator>;
    using HashProjectAccessor = value::MaterializedRowValueAccessor<TableType::iterator>;

    const std::vector<std::string> _outerCond;
    const std::vector<std::string> _outerProjects;
    const std::vector<std::string> _innerCond;
    const std::vector<std::string> _innerProjects;

    // all defined values from the outer side (i.e. they come from the hash table)
    std::map<std::string, value::SlotAccessor*, std::less<>> _outOuterAccessors;

    // accessors of input codition values (keys) that are being inserted into the hash table
    std::map<std::string, value::SlotAccessor*, std::less<>> _inOuterKeyAccessors;
    // accessors of output keys
    std::vector<std::unique_ptr<HashKeyAccessor>> _outOuterKeyAccessors;

    // accessors of input projection values that are build inserted into the hash table
    std::map<std::string, value::SlotAccessor*, std::less<>> _inOuterProjectAccessors;
    // accessors of output projections
    std::vector<std::unique_ptr<HashProjectAccessor>> _outOuterProjectAccessors;

    // accessors of input codition values (keys) that are being inserted into the hash table
    std::map<std::string, value::SlotAccessor*, std::less<>> _inInnerKeyAccessors;

    TableType _ht;
    TableType::iterator _htIt;
    TableType::iterator _htItEnd;

    vm::ByteCode _bytecode;

    bool _compiled{false};

public:
    HashJoinStage(std::unique_ptr<PlanStage> outer,
                  std::unique_ptr<PlanStage> inner,
                  const std::vector<std::string>& outerCond,
                  const std::vector<std::string>& outerProjects,
                  const std::vector<std::string>& innerCond,
                  const std::vector<std::string>& innerProjects);

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