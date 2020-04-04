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

#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/abt/free_vars.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
FDep::FDep(Type typeIn, std::vector<ABT> deps) : Base(std::move(deps)), _type(std::move(typeIn)) {
    checkValueSyntaxSort(nodes());
}

EvalPath::EvalPath(ABT pathIn, ABT inputIn)
    : Base(std::move(pathIn), std::move(inputIn)), _type(variantType()) {
    checkPathSyntaxSort(path());
    checkValueSyntaxSort(input());
}

FunctionCall::FunctionCall(std::string nameIn, std::vector<ABT> argsIn)
    : Base(std::move(argsIn)), _type(kNoType), _name(std::move(nameIn)) {
    checkValueSyntaxSort(nodes());
}

If::If(ABT condIn, ABT thenIn, ABT elseIn)
    : Base(std::move(condIn), std::move(thenIn), std::move(elseIn)) {
    checkValueSyntaxSort(get<0>());
    checkValueSyntaxSort(get<1>());
    checkValueSyntaxSort(get<2>());
}

BinaryOp::BinaryOp(Op opIn, ABT lhs, ABT rhs)
    : Base(std::move(lhs), std::move(rhs)), _type(kNoType), _op(opIn) {
    checkValueSyntaxSort(get<0>());
    checkValueSyntaxSort(get<1>());
}

UnaryOp::UnaryOp(Op opIn, ABT arg) : Base(std::move(arg)), _type(kNoType), _op(opIn) {
    checkValueSyntaxSort(get<0>());
}

LocalBind::LocalBind(ABT bindIn, ABT inIn) : Base(std::move(bindIn), std::move(inIn)) {
    uassert(ErrorCodes::InternalError, "binder expected", get<0>().is<ValueBinder>());
    checkValueSyntaxSort(get<1>());
}

LambdaAbstraction::LambdaAbstraction(ABT paramIn, ABT bodyIn)
    : Base(std::move(paramIn), std::move(bodyIn)), _type(kNoType) {
    uassert(ErrorCodes::InternalError, "binder expected", get<0>().is<ValueBinder>());
    checkValueSyntaxSort(get<1>());
}

OptFence::OptFence(ABT input) : Base(std::move(input)) {
    checkValueSyntaxSort(get<0>());
}

/**
 * Free variables
 */
ABT* FreeVariables::transport(ABT& e, LocalBind& op, ABT* bind, ABT* in) {
    mergeVarsHelper(&e, bind);
    // pull out free variables from the in expression
    mergeFreeVars(&e, in);
    // resolve free variables against current set of defined variables
    resolveVars(&e, &e);
    // drop locally bound variables as they cannot escape (good old lexical scoping)
    resetDefinedVars(&e);

    // this should be always empty?
    mergeDefinedVars(&e, in);

    return &e;
}

ABT* FreeVariables::transport(ABT& e, LambdaAbstraction& op, ABT* param, ABT* body) {
    mergeVarsHelper(&e, param);
    // pull out free variables from the in expression
    mergeFreeVars(&e, body);
    // resolve free variables against current set of defined variables
    resolveVars(&e, &e);
    // drop locally bound variables as they cannot escape (good old lexical scoping)
    resetDefinedVars(&e);

    // this should be always empty?
    mergeDefinedVars(&e, body);

    return &e;
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
