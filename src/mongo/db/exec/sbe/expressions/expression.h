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

#include "mongo/db/exec/sbe/util/debug_print.h"
#include "mongo/db/exec/sbe/values/value.h"
#include "mongo/db/exec/sbe/vm/vm.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mongo {
namespace sbe {
class PlanStage;
struct CompileCtx {
    PlanStage* root{nullptr};
    value::SlotAccessor* accumulator{nullptr};
    std::vector<std::pair<value::SlotId, value::SlotAccessor*>> correlated;
    bool aggExpression{false};

    value::SlotAccessor* getAccessor(value::SlotId slot);

    void pushCorrelated(value::SlotId slot, value::SlotAccessor* accessor);
    void popCorrelated();
};

class EExpression {
protected:
    std::vector<std::unique_ptr<EExpression>> _nodes;

public:
    virtual ~EExpression() {}

    // This is unspeakably ugly
    virtual std::unique_ptr<EExpression> clone() = 0;

    virtual std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) = 0;

    virtual std::vector<DebugPrinter::Block> debugPrint() = 0;
};

template <typename T, typename... Args>
inline std::unique_ptr<EExpression> makeE(Args&&... args) {
    return std::make_unique<T>(std::forward<Args>(args)...);
}

template <typename... Ts>
inline std::vector<std::unique_ptr<EExpression>> makeEs(Ts&&... pack) {
    std::vector<std::unique_ptr<EExpression>> exprs;

    (exprs.emplace_back(std::forward<Ts>(pack)), ...);

    return exprs;
}

namespace detail {
// base case
inline void makeEM_unwind(std::unordered_map<value::SlotId, std::unique_ptr<EExpression>>& result,
                          value::SlotId slot,
                          std::unique_ptr<EExpression> expr) {
    result.emplace(slot, std::move(expr));
}

// recursive case
template <typename... Ts>
inline void makeEM_unwind(std::unordered_map<value::SlotId, std::unique_ptr<EExpression>>& result,
                          value::SlotId slot,
                          std::unique_ptr<EExpression> expr,
                          Ts&&... rest) {
    result.emplace(slot, std::move(expr));
    makeEM_unwind(result, std::forward<Ts>(rest)...);
}
}  // namespace detail

template <typename... Ts>
auto makeEM(Ts&&... pack) {
    std::unordered_map<value::SlotId, std::unique_ptr<EExpression>> result;
    detail::makeEM_unwind(result, std::forward<Ts>(pack)...);
    return result;
}

class EConstant final : public EExpression {
    value::TypeTags _tag;
    value::Value _val;
    std::string _s;

    bool _owned{true};

public:
    EConstant(value::TypeTags tag, value::Value val) : _tag(tag), _val(val) {}
    EConstant(std::string_view s) : _s(s) {
        _owned = false;

        _tag = value::TypeTags::StringBig;
        _val = value::bitcastFrom(_s.c_str());
    }

    ~EConstant() {
        if (_owned)
            value::releaseValue(_tag, _val);
    }

    std::unique_ptr<EExpression> clone() override;

    std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};

class EVariable final : public EExpression {
    value::SlotId _var;

public:
    EVariable(value::SlotId var) : _var(var) {}

    std::unique_ptr<EExpression> clone() override;

    std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};

class EPrimBinary final : public EExpression {
public:
    enum Op {
        add,
        sub,

        mul,
        div,

        lessEq,
        less,
        greater,
        greaterEq,

        eq,
        neq,

        cmp3w,

        logicAnd,
        logicOr,
    };

private:
    Op _op;

public:
    EPrimBinary(Op op, std::unique_ptr<EExpression> lhs, std::unique_ptr<EExpression> rhs)
        : _op(op) {
        _nodes.emplace_back(std::move(lhs));
        _nodes.emplace_back(std::move(rhs));
    }

    std::unique_ptr<EExpression> clone() override;

    std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};

class EPrimUnary final : public EExpression {
public:
    enum Op {
        negate,
        logicNot,
    };

private:
    Op _op;

public:
    EPrimUnary(Op op, std::unique_ptr<EExpression> operand) : _op(op) {
        _nodes.emplace_back(std::move(operand));
    }

    std::unique_ptr<EExpression> clone() override;

    std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};

class EFunction final : public EExpression {
    std::string _name;

public:
    EFunction(std::string_view name, std::vector<std::unique_ptr<EExpression>> args) : _name(name) {
        _nodes = std::move(args);
    }

    std::unique_ptr<EExpression> clone() override;

    std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};

class EIf final : public EExpression {
public:
    EIf(std::unique_ptr<EExpression> cond,
        std::unique_ptr<EExpression> thenBranch,
        std::unique_ptr<EExpression> elseBranch) {
        _nodes.emplace_back(std::move(cond));
        _nodes.emplace_back(std::move(thenBranch));
        _nodes.emplace_back(std::move(elseBranch));
    }

    std::unique_ptr<EExpression> clone() override;

    std::unique_ptr<vm::CodeFragment> compile(CompileCtx& ctx) override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};
}  // namespace sbe
}  // namespace mongo
