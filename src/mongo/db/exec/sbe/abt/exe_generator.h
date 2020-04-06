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

#include "mongo/db/exec/sbe/abt/base.h"
#include "mongo/db/exec/sbe/values/slot_id_generator.h"

namespace mongo {
namespace sbe {
class PlanStage;
class EExpression;
namespace abt {
/**
 * SBE execution plan generator
 */
class ExeGenerator {
    struct SlotInfo {
        boost::optional<value::SlotId> slot;
        boost::optional<FrameId> frame;
    };
    struct GenResult {
        std::unique_ptr<PlanStage> stage;
        std::unique_ptr<EExpression> expr;
        std::vector<std::unique_ptr<EExpression>> exprs;
    };

    struct PathContext {
        std::unique_ptr<EExpression> expr;
        boost::optional<value::SlotId> slot;

        bool topLevelTraverse{false};

        std::vector<std::string> restrictFields;
        std::vector<std::string> projectFields;
        std::vector<value::SlotId> projectVars;
        bool forceNewObject{false};
        bool returnOldObject{true};
    };
    const ABT& _root;

    value::IdGenerator<value::SlotId> _slotIdGen;
    value::IdGenerator<FrameId> _frameIdGen;
    std::unique_ptr<PathContext> _pathCtx;
    std::unique_ptr<std::vector<std::unique_ptr<EExpression>>> _lambdaCtx;

    std::unique_ptr<PlanStage> _currentStage;
    std::unordered_map<const ValueBinder*, std::vector<SlotInfo>> _slots;

    friend class SlotAllocator;

    GenResult generateInputPhase(VarId id, const ABT& body);
    GenResult generateOutputPhase(VarId id, const ABT& body);
    auto generateDeps(const std::vector<ABT>& deps) {
        std::vector<GenResult> resultDeps;
        for (const auto& d : deps) {
            resultDeps.emplace_back(generate(d));
        }
        return resultDeps;
    }
    GenResult generateBind(bool asExpression, const SlotInfo& info, const ABT& e);
    void generatePathMkObj(value::SlotId inputSlot);


public:
    value::SlotId getSlot(const ValueBinder*, VarId id);

    ExeGenerator(const ABT& root) : _root(root) {}

    // Quick and dirty hack - sometime we link with the storage engine and sometime not.
    using ScanWalkFnType = std::function<GenResult(ExeGenerator&, const Scan& op, const ABT& body)>;
    static ScanWalkFnType _scanImpl;

    GenResult generate(const ABT& e);
    GenResult generate();

    GenResult walk(const Blackhole& op);
    GenResult walk(const Constant& op);
    GenResult walk(const ConstantMagic& op);
    GenResult walk(const FDep& op, const std::vector<ABT>& deps);
    GenResult walk(const EvalPath& op, const ABT& path, const ABT& input);
    GenResult walk(const FunctionCall& op, const std::vector<ABT>& args);
    GenResult walk(const If& op, const ABT& cond, const ABT& thenBranch, const ABT& elseBranch);
    GenResult walk(const BinaryOp& op, const ABT& lhs, const ABT& rhs);
    GenResult walk(const UnaryOp& op, const ABT& arg);
    GenResult walk(const LocalBind& op, const ABT& bind, const ABT& in);
    GenResult walk(const LambdaAbstraction& op, const ABT& param, const ABT& in);
    GenResult walk(const BoundParameter& op);
    GenResult walk(const OptFence& op, const ABT& arg);
    GenResult walk(const Variable& op);

    GenResult walk(const ValueBinder& op, const std::vector<ABT>& binds);

    GenResult walk(const PathIdentity& op);
    GenResult walk(const PathConstant& op, const ABT& c);
    GenResult walk(const PathLambda& op, const ABT& lam);
    GenResult walk(const PathDrop& op);
    GenResult walk(const PathKeep& op);
    GenResult walk(const PathObj& op);
    GenResult walk(const PathTraverse& op, const ABT& c);
    GenResult walk(const PathField& op, const ABT& c);
    GenResult walk(const PathGet& op, const ABT& c);
    GenResult walk(const PathCompose& op, const ABT& t2, const ABT& t1);

    GenResult walk(const Scan& op, const ABT& body);
    GenResult walk(const Unwind& op, const std::vector<ABT>& deps, const ABT& body);
    GenResult walk(const Join& op, const std::vector<ABT>& deps, const ABT& body);
    GenResult walk(const Filter& op, const std::vector<ABT>& deps, const ABT& body);
    GenResult walk(const Group& op, const std::vector<ABT>& deps, const ABT& body);
    GenResult walk(const Facet& op, const std::vector<ABT>& deps, const ABT& body);
    GenResult walk(const Sort& op, const std::vector<ABT>& deps, const ABT& body);
    GenResult walk(const Exchange& op, const std::vector<ABT>& deps, const ABT& body);

    GenResult walkImpl(const Scan& op, const ABT& body);
};
}  // namespace abt
}  // namespace sbe
}  // namespace mongo