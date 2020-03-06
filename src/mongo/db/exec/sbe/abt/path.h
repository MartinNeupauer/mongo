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
#include "mongo/util/assert_util.h"

#include <string>

namespace mongo {
namespace sbe {
namespace abt {

class PathSyntaxSort {
public:
    virtual ~PathSyntaxSort() {}
    virtual const ABT& type() const = 0;
};
/**
 * Reflects path selected values, not the paths themselves.
 * This could also be modeled as \x->x but we would need lambda expressions and it would complicate
 * patter matching during optimization.
 */
class PathIdentity : public Operator<PathIdentity, 1>, public PathSyntaxSort {
    using Base = Operator<PathIdentity, 1>;

public:
    const ABT& type() const override {
        return get<0>();
    }

    PathIdentity(ABT typeIn) : Base(std::move(typeIn)) {
        invariant(type().is<TypeSyntaxSort>());
    }
};

/**
 * Path selected value is expression.
 * Type V - simply evaluate V.
 * Type V->V - pass the current selected value and evaluate the function.
 */
class PathExpression : public Operator<PathExpression, 2>, public PathSyntaxSort {
    using Base = Operator<PathExpression, 2>;

public:
    const ABT& type() const override {
        return get<0>();
    }
    const auto& expr() const {
        return get<1>();
    }
    PathExpression(ABT typeIn, ABT exprIn) : Base(std::move(typeIn), std::move(exprIn)) {
        invariant(type().is<TypeSyntaxSort>());
        invariant(expr().is<ValueSyntaxSort>());
    }
};

/**
 * Drop/restrict the value. Could be modeled as \_->Nothing
 */
class PathRestrict : public Operator<PathRestrict, 1>, public PathSyntaxSort {
    using Base = Operator<PathRestrict, 1>;

public:
    const ABT& type() const override {
        return get<0>();
    }

    PathRestrict(ABT typeIn) : Base(std::move(typeIn)) {
        invariant(type().is<TypeSyntaxSort>());
    }
};

enum class FieldSelector { direct, traverseDeep, traverseShallow };
enum class FieldApply { always, ifObject };

class PathFieldApply : public Operator<PathFieldApply, 1>, public PathSyntaxSort {
    using Base = Operator<PathFieldApply, 1>;

    std::string _fieldName;
    FieldSelector _traverseType;
    FieldApply _applyType;

public:
    const auto& apply() const {
        return get<0>();
    }

    const ABT& type() const override {
        return apply().cast<PathSyntaxSort>()->type();
    }

    PathFieldApply(std::string fieldNameIn,
                   FieldSelector traverseTypeIn,
                   FieldApply applyTypeIn,
                   ABT applyIn)
        : Base(std::move(applyIn)),
          _fieldName(std::move(fieldNameIn)),
          _traverseType(traverseTypeIn),
          _applyType(applyTypeIn) {
        invariant(apply().is<PathSyntaxSort>());
    }
};

class PathObject : public OperatorDynamic<PathObject, 0>, public PathSyntaxSort {
    using Base = OperatorDynamic<PathObject, 0>;

public:
    const auto& selectors() const {
        return nodes();
    }

    // const ABT& type() const override {/* ??? */}

    PathObject(std::vector<ABT> fieldsIn) : Base(std::move(fieldsIn)) {
        for (const auto& s : selectors()) {
            invariant(s.is<PathFieldApply>());
        }
    }
};

class PathValue : public Operator<PathValue, 1>, public PathSyntaxSort {
    using Base = Operator<PathValue, 1>;

public:
    const auto& selector() const {
        return get<0>();
    }

    // const ABT& type() const override {/* ??? */}

    PathValue(ABT fieldIn) : Base(std::move(fieldIn)) {
        invariant(selector().is<PathFieldApply>());
    }
};
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
