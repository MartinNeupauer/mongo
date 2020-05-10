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

#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/spool.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/util/str.h"

#include <sstream>

namespace mongo {
namespace sbe {
template <typename F>
std::unique_ptr<vm::CodeFragment> wrapNothingTest(std::unique_ptr<vm::CodeFragment> code, F&& f) {
    auto inner = std::make_unique<vm::CodeFragment>();
    inner = f(std::move(inner));

    invariant(inner->stackSize() == 0);

    // append the jump
    code->appendJumpNothing(inner->instrs().size());

    // append the inner block
    code->append(std::move(inner));

    return code;
}
std::unique_ptr<EExpression> EConstant::clone() {
    if (_s.empty()) {
        auto [tag, val] = value::copyValue(_tag, _val);
        return std::make_unique<EConstant>(tag, val);
    } else {
        return std::make_unique<EConstant>(_s);
    }
}
std::unique_ptr<vm::CodeFragment> EConstant::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    code->appendConstVal(_tag, _val);

    return code;
}
std::vector<DebugPrinter::Block> EConstant::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    std::stringstream ss;
    value::printValue(ss, _tag, _val);

    ret.emplace_back(ss.str());

    return ret;
}
std::unique_ptr<EExpression> EVariable::clone() {
    return _frameId ? std::make_unique<EVariable>(*_frameId, _var)
                    : std::make_unique<EVariable>(_var);
}
std::unique_ptr<vm::CodeFragment> EVariable::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    if (_frameId) {
        int offset = -_var - 1;
        code->appendLocalVal(*_frameId, offset);
    } else {
        auto accessor = ctx.root->getAccessor(ctx, _var);
        code->appendAccessVal(accessor);
    }

    return code;
}
std::vector<DebugPrinter::Block> EVariable::debugPrint() {
    std::vector<DebugPrinter::Block> ret;

    if (_frameId) {
        DebugPrinter::addIdentifier(ret, *_frameId, _var);
    } else {
        DebugPrinter::addIdentifier(ret, _var);
    }

    return ret;
}
std::unique_ptr<EExpression> EPrimBinary::clone() {
    return std::make_unique<EPrimBinary>(_op, _nodes[0]->clone(), _nodes[1]->clone());
}
std::unique_ptr<vm::CodeFragment> EPrimBinary::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    auto lhs = _nodes[0]->compile(ctx);
    auto rhs = _nodes[1]->compile(ctx);

    switch (_op) {
        case EPrimBinary::add:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendAdd();
            break;
        case EPrimBinary::sub:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendSub();
            break;
        case EPrimBinary::mul:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendMul();
            break;
        case EPrimBinary::div:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendDiv();
            break;
        case EPrimBinary::less:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendLess();
            break;
        case EPrimBinary::lessEq:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendLessEq();
            break;
        case EPrimBinary::greater:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendGreater();
            break;
        case EPrimBinary::greaterEq:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendGreaterEq();
            break;
        case EPrimBinary::eq:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendEq();
            break;
        case EPrimBinary::neq:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendNeq();
            break;
        case EPrimBinary::cmp3w:
            code->append(std::move(lhs));
            code->append(std::move(rhs));
            code->appendCmp3w();
            break;
        case EPrimBinary::logicAnd: {
            auto codeFalseBranch = std::make_unique<vm::CodeFragment>();
            codeFalseBranch->appendConstVal(value::TypeTags::Boolean, false);
            // jump to the merge point that will be right after the thenBranch (rhs)
            codeFalseBranch->appendJump(rhs->instrs().size());

            code->append(std::move(lhs));
            code = wrapNothingTest(std::move(code), [&](std::unique_ptr<vm::CodeFragment> code) {
                // jump if true
                code->appendJumpTrue(codeFalseBranch->instrs().size());
                // append the false branch and the rhs branch
                code->append(std::move(codeFalseBranch), std::move(rhs));

                return code;
            });
            break;
        }
        case EPrimBinary::logicOr: {
            auto codeTrueBranch = std::make_unique<vm::CodeFragment>();
            codeTrueBranch->appendConstVal(value::TypeTags::Boolean, true);

            // jump to the merge point that will be right after the thenBranch (true branch)
            rhs->appendJump(codeTrueBranch->instrs().size());

            code->append(std::move(lhs));
            code = wrapNothingTest(std::move(code), [&](std::unique_ptr<vm::CodeFragment> code) {
                // jump if true
                code->appendJumpTrue(rhs->instrs().size());
                // append the false branch and the true branch
                code->append(std::move(rhs), std::move(codeTrueBranch));

                return code;
            });
            break;
        }
        default:
            invariant(Status(ErrorCodes::InternalError, "not yet implemented"));
            break;
    }
    return code;
}
std::vector<DebugPrinter::Block> EPrimBinary::debugPrint() {
    std::vector<DebugPrinter::Block> ret;

    DebugPrinter::addBlocks(ret, _nodes[0]->debugPrint());

    switch (_op) {
        case EPrimBinary::add:
            ret.emplace_back("+");
            break;
        case EPrimBinary::sub:
            ret.emplace_back("-");
            break;
        case EPrimBinary::mul:
            ret.emplace_back("*");
            break;
        case EPrimBinary::div:
            ret.emplace_back("/");
            break;
        case EPrimBinary::less:
            ret.emplace_back("<");
            break;
        case EPrimBinary::lessEq:
            ret.emplace_back("<=");
            break;
        case EPrimBinary::greater:
            ret.emplace_back(">");
            break;
        case EPrimBinary::greaterEq:
            ret.emplace_back(">=");
            break;
        case EPrimBinary::eq:
            ret.emplace_back("==");
            break;
        case EPrimBinary::neq:
            ret.emplace_back("!=");
            break;
        case EPrimBinary::cmp3w:
            ret.emplace_back("<=>");
            break;
        case EPrimBinary::logicAnd:
            ret.emplace_back("&&");
            break;
        case EPrimBinary::logicOr:
            ret.emplace_back("||");
            break;
        default:
            invariant(Status(ErrorCodes::InternalError, "not yet implemented"));
            break;
    }
    DebugPrinter::addBlocks(ret, _nodes[1]->debugPrint());

    return ret;
}
std::unique_ptr<EExpression> EPrimUnary::clone() {
    return std::make_unique<EPrimUnary>(_op, _nodes[0]->clone());
}
std::unique_ptr<vm::CodeFragment> EPrimUnary::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    auto operand = _nodes[0]->compile(ctx);

    switch (_op) {
        case negate:
            code->append(std::move(operand));
            code->appendNegate();
            break;
        case EPrimUnary::logicNot:
            code->append(std::move(operand));
            code->appendNot();
            break;
        default:
            invariant(Status(ErrorCodes::InternalError, "not yet implemented"));
            break;
    }
    return code;
}
std::vector<DebugPrinter::Block> EPrimUnary::debugPrint() {
    std::vector<DebugPrinter::Block> ret;

    switch (_op) {
        case EPrimUnary::negate:
            ret.emplace_back("-");
            break;
        case EPrimUnary::logicNot:
            ret.emplace_back("!");
            break;
        default:
            invariant(Status(ErrorCodes::InternalError, "not yet implemented"));
            break;
    }

    DebugPrinter::addBlocks(ret, _nodes[0]->debugPrint());

    return ret;
}
std::unique_ptr<EExpression> EFunction::clone() {
    std::vector<std::unique_ptr<EExpression>> args;
    for (auto& a : _nodes) {
        args.emplace_back(a->clone());
    }
    return std::make_unique<EFunction>(_name, std::move(args));
}
namespace {
/**
 * The arity test function. It returns true if the number of arguments is correct.
 */
using ArityFn = bool (*)(size_t);

/**
 * The builtin function description.
 */
struct BuiltinFn {
    ArityFn arityTest;
    vm::Builtin builtin;
};

/**
 * The map of recognized builtin functions.
 */
static stdx::unordered_map<std::string, BuiltinFn> kBuiltinFunctions = {
    {"split", BuiltinFn{[](size_t n) { return n == 2; }, vm::Builtin::split}},
    {"dropFields", BuiltinFn{[](size_t n) { return n > 0; }, vm::Builtin::dropFields}},
    {"newObj", BuiltinFn{[](size_t n) { return n % 2 == 0; }, vm::Builtin::newObj}},
    {"ksToString", BuiltinFn{[](size_t n) { return n == 1; }, vm::Builtin::ksToString}},
    {"ks", BuiltinFn{[](size_t n) { return n > 2; }, vm::Builtin::newKs}},
    {"abs", BuiltinFn{[](size_t n) { return n == 1; }, vm::Builtin::abs}},
};

/**
 * The code generation function.
 */
using CodeFn = void (vm::CodeFragment::*)();

/**
 * The function description.
 */
struct InstrFn {
    ArityFn arityTest;
    CodeFn generate;
    bool aggregate;
};
/**
 * The map of functions that resolve directly to instructions.
 */
static stdx::unordered_map<std::string, InstrFn> kInstrFunctions = {
    {"getField",
     InstrFn{[](size_t n) { return n == 2; }, &vm::CodeFragment::appendGetField, false}},
    {"fillEmpty",
     InstrFn{[](size_t n) { return n == 2; }, &vm::CodeFragment::appendFillEmpty, false}},
    {"exists", InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendExists, false}},
    {"isNull", InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendIsNull, false}},
    {"isObject",
     InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendIsObject, false}},
    {"isArray", InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendIsArray, false}},
    {"isString",
     InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendIsString, false}},
    {"isNumber",
     InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendIsNumber, false}},
    {"sum", InstrFn{[](size_t n) { return n == 1; }, &vm::CodeFragment::appendSum, true}},
};
}  // namespace

std::unique_ptr<vm::CodeFragment> EFunction::compile(CompileCtx& ctx) {
    if (auto it = kBuiltinFunctions.find(_name); it != kBuiltinFunctions.end()) {
        if (!it->second.arityTest(_nodes.size())) {
            uasserted(ErrorCodes::InternalError,
                      str::stream()
                          << "function call: " << _name << " has wrong arity: " << _nodes.size());
        }
        auto code = std::make_unique<vm::CodeFragment>();

        for (size_t idx = _nodes.size(); idx-- > 0;) {
            code->append(_nodes[idx]->compile(ctx));
        }

        code->appendFunction(it->second.builtin, _nodes.size());

        return code;
    }
    if (auto it = kInstrFunctions.find(_name); it != kInstrFunctions.end()) {
        if (!it->second.arityTest(_nodes.size())) {
            uasserted(ErrorCodes::InternalError,
                      str::stream()
                          << "function call: " << _name << " has wrong arity: " << _nodes.size());
        }
        auto code = std::make_unique<vm::CodeFragment>();

        if (it->second.aggregate) {
            uassert(ErrorCodes::InternalError,
                    str::stream() << "aggregate function call: " << _name
                                  << " occurs in the non-aggregate context.",
                    ctx.aggExpression);

            code->appendAccessVal(ctx.accumulator);
        }
        for (size_t idx = 0; idx < _nodes.size(); ++idx) {
            code->append(_nodes[idx]->compile(ctx));
        }
        (*code.*(it->second.generate))();

        return code;
    }

    uasserted(ErrorCodes::InternalError, str::stream() << "unknown function call: " << _name);
}
std::vector<DebugPrinter::Block> EFunction::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, _name);

    ret.emplace_back("(`");
    for (size_t idx = 0; idx < _nodes.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addBlocks(ret, _nodes[idx]->debugPrint());
    }
    ret.emplace_back("`)");

    return ret;
}

std::unique_ptr<EExpression> EIf::clone() {
    return std::make_unique<EIf>(_nodes[0]->clone(), _nodes[1]->clone(), _nodes[2]->clone());
}
std::unique_ptr<vm::CodeFragment> EIf::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    auto thenBranch = _nodes[1]->compile(ctx);

    auto elseBranch = _nodes[2]->compile(ctx);

    // then and else branches must be balanced
    invariant(thenBranch->stackSize() == elseBranch->stackSize());

    // jump to the merge point that will be right after the thenBranch
    elseBranch->appendJump(thenBranch->instrs().size());

    // compile the condition
    code->append(_nodes[0]->compile(ctx));
    code = wrapNothingTest(std::move(code), [&](std::unique_ptr<vm::CodeFragment> code) {
        // jump around the elseBranch
        code->appendJumpTrue(elseBranch->instrs().size());
        // append else and then branches
        code->append(std::move(elseBranch), std::move(thenBranch));

        return code;
    });
    return code;
}
std::vector<DebugPrinter::Block> EIf::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "if");

    ret.emplace_back("(`");

    // condition
    DebugPrinter::addBlocks(ret, _nodes[0]->debugPrint());
    ret.emplace_back(DebugPrinter::Block("`,"));
    // thenBranch
    DebugPrinter::addBlocks(ret, _nodes[1]->debugPrint());
    ret.emplace_back(DebugPrinter::Block("`,"));
    // elseBranch
    DebugPrinter::addBlocks(ret, _nodes[2]->debugPrint());

    ret.emplace_back("`)");

    return ret;
}

std::unique_ptr<EExpression> ELocalBind::clone() {
    std::vector<std::unique_ptr<EExpression>> binds;
    for (size_t idx = 0; idx < _nodes.size() - 1; ++idx) {
        binds.emplace_back(_nodes[idx]->clone());
    }
    return std::make_unique<ELocalBind>(_frameId, std::move(binds), _nodes.back()->clone());
}

std::unique_ptr<vm::CodeFragment> ELocalBind::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    for (size_t idx = 0; idx < _nodes.size(); ++idx) {
        auto c = _nodes[idx]->compile(ctx);
        code->append(std::move(c));
    }

    for (size_t idx = 0; idx < _nodes.size() - 1; ++idx) {
        code->appendSwap();
        code->appendPop();
    }
    code->removeFixup(_frameId);
    return code;
}

std::vector<DebugPrinter::Block> ELocalBind::debugPrint() {
    std::vector<DebugPrinter::Block> ret;

    DebugPrinter::addKeyword(ret, "let");

    ret.emplace_back("[`");
    for (size_t idx = 0; idx < _nodes.size() - 1; ++idx) {
        if (idx != 0) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _frameId, idx);
        ret.emplace_back("=");
        DebugPrinter::addBlocks(ret, _nodes[idx]->debugPrint());
    }
    ret.emplace_back("`]");

    DebugPrinter::addBlocks(ret, _nodes.back()->debugPrint());

    return ret;
}

std::unique_ptr<EExpression> EFail::clone() {
    return std::make_unique<EFail>(_code, _message);
}
std::unique_ptr<vm::CodeFragment> EFail::compile(CompileCtx& ctx) {
    auto code = std::make_unique<vm::CodeFragment>();

    code->appendConstVal(value::TypeTags::NumberInt64,
                         value::bitcastFrom(static_cast<int64_t>(_code)));

    code->appendConstVal(value::TypeTags::StringBig, value::bitcastFrom(_message.c_str()));

    code->appendFail();

    return code;
}
std::vector<DebugPrinter::Block> EFail::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "fail");

    ret.emplace_back("(");

    ret.emplace_back(DebugPrinter::Block(std::to_string(_code)));
    ret.emplace_back(DebugPrinter::Block(",`"));
    ret.emplace_back(DebugPrinter::Block(_message));

    ret.emplace_back("`)");

    return ret;
}

value::SlotAccessor* CompileCtx::getAccessor(value::SlotId slot) {
    for (auto it = correlated.rbegin(); it != correlated.rend(); ++it) {
        if (it->first == slot) {
            return it->second;
        }
    }

    uasserted(ErrorCodes::InternalError, str::stream() << "undefined slot accessor:" << slot);
}

std::shared_ptr<SpoolBuffer> CompileCtx::getSpoolBuffer(SpoolId spool) {
    if (spoolBuffers.find(spool) == spoolBuffers.end()) {
        spoolBuffers.emplace(spool, std::make_shared<SpoolBuffer>());
    }
    return spoolBuffers[spool];
}

void CompileCtx::pushCorrelated(value::SlotId slot, value::SlotAccessor* accessor) {
    correlated.emplace_back(slot, accessor);
}

void CompileCtx::popCorrelated() {
    correlated.pop_back();
}
}  // namespace sbe
}  // namespace mongo
