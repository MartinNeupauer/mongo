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
#include "mongo/db/exec/sbe/values/value.h"

#include <map>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

namespace mongo {
namespace sbe {
class MakeObjStage final : public PlanStage {
    const std::string _objName;
    const std::string _rootName;
    const std::vector<std::string> _restrictFields;
    const std::vector<std::string> _projectFields;
    const std::vector<std::string> _projectVarNames;
    std::set<std::string, std::less<>> _restrictFieldsSet;
    std::set<std::string, std::less<>> _projectFieldsSet;

    std::vector<std::pair<std::string, value::SlotAccessor*>> _projects;

    value::OwnedValueAccessor _obj;

    value::SlotAccessor* _root{nullptr};

    bool _compiled{false};

public:
    MakeObjStage(std::unique_ptr<PlanStage> input,
                 std::string_view objName,
                 std::string_view rootName,
                 const std::vector<std::string>& restrictFields,
                 const std::vector<std::string>& projectFields,
                 const std::vector<std::string>& projectVarNames);

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