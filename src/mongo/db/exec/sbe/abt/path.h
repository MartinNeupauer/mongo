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
#include "mongo/util/assert_util.h"

#include <string>
#include <unordered_set>

namespace mongo {
namespace sbe {
namespace abt {

class PathSyntaxSort {
public:
    virtual ~PathSyntaxSort() {}
    virtual const Type& type() const = 0;
};
/**
 * Reflects path selected values, not the paths themselves.
 * This could also be modeled as \x->x but it would complicate patter matching during
 * optimization.
 */
class PathIdentity final : public Operator<PathIdentity, 0>, public PathSyntaxSort {
    using Base = Operator<PathIdentity, 0>;

public:
    const Type& type() const override {
        return kVariantType;
    }

    PathIdentity() {}
};

/**
 * This could also be modeled as \_->c but it would complicate patter matching during
 * optimization. The c can be any expression returning a variant value.
 */
class PathConstant final : public Operator<PathConstant, 1>, public PathSyntaxSort {
    using Base = Operator<PathConstant, 1>;

public:
    const Type& type() const override {
        return kVariantType;
    }

    PathConstant(ABT c);
};

/**
 * The generic \x->... The c must be V->V lambda abstraction.
 */
class PathLambda final : public Operator<PathLambda, 1>, public PathSyntaxSort {
    using Base = Operator<PathLambda, 1>;

public:
    const Type& type() const override {
        return kVariantType;
    }

    PathLambda(ABT c);
};

/**
 * Drop the fields _names from the input.
 */
class PathDrop final : public Operator<PathDrop, 0>, public PathSyntaxSort {
    using Base = Operator<PathDrop, 0>;

    std::unordered_set<std::string> _names;

public:
    const Type& type() const override {
        return kVariantType;
    }

    PathDrop(std::unordered_set<std::string> names) : _names(std::move(names)) {}
};

/**
 * Keep the fields _names from the input and drop the rest.
 */
class PathKeep final : public Operator<PathKeep, 0>, public PathSyntaxSort {
    using Base = Operator<PathKeep, 0>;

    std::unordered_set<std::string> _names;

public:
    const Type& type() const override {
        return kVariantType;
    }

    PathKeep(std::unordered_set<std::string> names) : _names(std::move(names)) {}
};

/**
 * Force the path to always evalute to an object.
 */
class PathObj final : public Operator<PathObj, 0>, public PathSyntaxSort {
    using Base = Operator<PathObj, 0>;

public:
    const Type& type() const override {
        return kVariantType;
    }

    PathObj() {}
};

/**
 * Nested arrays traversals.
 */
class PathTraverse final : public Operator<PathTraverse, 1>, public PathSyntaxSort {
    using Base = Operator<PathTraverse, 1>;

public:
    const auto& t() const {
        return get<0>();
    }
    const Type& type() const override {
        return t().cast<PathSyntaxSort>()->type();
    }

    PathTraverse(ABT c);
};

/**
 * Follow the field.
 */
class PathField final : public Operator<PathField, 1>, public PathSyntaxSort {
    using Base = Operator<PathField, 1>;

    std::string _name;

public:
    const auto& t() const {
        return get<0>();
    }
    const Type& type() const override {
        return t().cast<PathSyntaxSort>()->type();
    }

    PathField(std::string nameIn, ABT c);
};

/**
 * Get the field value.
 */
class PathGet final : public Operator<PathGet, 1>, public PathSyntaxSort {
    using Base = Operator<PathGet, 1>;

    std::string _name;

public:
    const auto& t() const {
        return get<0>();
    }
    const Type& type() const override {
        return t().cast<PathSyntaxSort>()->type();
    }

    PathGet(std::string nameIn, ABT c);
};

/**
 * Path composition.
 */
class PathCompose final : public Operator<PathCompose, 2>, public PathSyntaxSort {
    using Base = Operator<PathCompose, 2>;

public:
    const auto& t2() const {
        return get<0>();
    }
    const auto& t1() const {
        return get<1>();
    }
    const Type& type() const override {
        return t1().cast<PathSyntaxSort>()->type();
    }

    PathCompose(ABT t2In, ABT t1In);
};

}  // namespace abt
}  // namespace sbe
}  // namespace mongo
