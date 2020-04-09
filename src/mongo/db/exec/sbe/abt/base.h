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

#include "mongo/db/exec/sbe/abt/type.h"

namespace mongo {
namespace sbe {
namespace abt {
using VarId = int64_t;

/**
 * Path sort
 */
class PathIdentity;
class PathConstant;
class PathLambda;
class PathDrop;
class PathKeep;
class PathObj;
class PathTraverse;
class PathField;
class PathGet;
class PathCompose;
/**
 * Value sort
 */
class Blackhole;
class Constant;
class ConstantMagic;
class Variable;
class EvalPath;
class FDep;  // functional dependency
class FunctionCall;
class If;
class BinaryOp;
class UnaryOp;
class LocalBind;
class LambdaAbstraction;
class BoundParameter;
class OptFence;
/**
 * Relational operators/boxes sort
 */
class Scan;
class Unwind;
class Join;
class Filter;
class Group;
class Facet;
class Sort;
class Exchange;

class ValueBinder;

using ABT = algebra::PolyValue<Blackhole,
                               Constant,
                               ConstantMagic,
                               Variable,
                               EvalPath,
                               FDep,
                               FunctionCall,
                               If,
                               BinaryOp,
                               UnaryOp,
                               LocalBind,
                               LambdaAbstraction,
                               BoundParameter,
                               OptFence,

                               PathIdentity,
                               PathConstant,
                               PathLambda,
                               PathDrop,
                               PathKeep,
                               PathObj,
                               PathTraverse,
                               PathField,
                               PathGet,
                               PathCompose,

                               Scan,
                               Unwind,
                               Join,
                               Filter,
                               Group,
                               Facet,
                               Sort,
                               Exchange,
                               ValueBinder>;

template <typename Derived, size_t Arity>
using Operator = algebra::OpSpecificArity<ABT, Derived, Arity>;

template <typename Derived, size_t Arity>
using OperatorDynamic = algebra::OpSpecificDynamicArity<ABT, Derived, Arity>;

template <typename T, typename... Args>
inline auto make(Args&&... args) {
    return ABT::make<T>(std::forward<Args>(args)...);
}

template <typename... Args>
inline auto makeSeq(Args&&... args) {
    std::vector<ABT> seq;
    (seq.emplace_back(std::forward<Args>(args)), ...);
    return seq;
}
class ValueSyntaxSort {
public:
    virtual ~ValueSyntaxSort() {}
    virtual const Type& type() const = 0;
};
class OpSyntaxSort {
public:
    virtual ~OpSyntaxSort() {}
    virtual ABT& body() = 0;
};

void checkValueSyntaxSort(const ABT& n);
void checkOpSyntaxSort(const ABT& n);
void checkPathSyntaxSort(const ABT& n);
void checkBinder(const ABT& n);
void checkValueSyntaxSort(const std::vector<ABT>& n);
void checkOpSyntaxSort(const std::vector<ABT>& n);

const ABT& follow(const ABT& input);
ABT& follow(ABT& input);

}  // namespace abt
}  // namespace sbe
}  // namespace mongo