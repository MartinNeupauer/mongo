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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/sbe_stage_builder.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/ix_scan.h"
#include "mongo/db/exec/sbe/stages/limit.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_array.h"
#include "mongo/db/matcher/expression_expr.h"
#include "mongo/db/matcher/expression_geo.h"
#include "mongo/db/matcher/expression_internal_expr_eq.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/expression_text.h"
#include "mongo/db/matcher/expression_text_noop.h"
#include "mongo/db/matcher/expression_tree.h"
#include "mongo/db/matcher/expression_type.h"
#include "mongo/db/matcher/expression_visitor.h"
#include "mongo/db/matcher/expression_where.h"
#include "mongo/db/matcher/expression_where_noop.h"
#include "mongo/db/matcher/schema/expression_internal_schema_all_elem_match_from_index.h"
#include "mongo/db/matcher/schema/expression_internal_schema_allowed_properties.h"
#include "mongo/db/matcher/schema/expression_internal_schema_cond.h"
#include "mongo/db/matcher/schema/expression_internal_schema_eq.h"
#include "mongo/db/matcher/schema/expression_internal_schema_fmod.h"
#include "mongo/db/matcher/schema/expression_internal_schema_match_array_index.h"
#include "mongo/db/matcher/schema/expression_internal_schema_max_items.h"
#include "mongo/db/matcher/schema/expression_internal_schema_max_length.h"
#include "mongo/db/matcher/schema/expression_internal_schema_max_properties.h"
#include "mongo/db/matcher/schema/expression_internal_schema_min_items.h"
#include "mongo/db/matcher/schema/expression_internal_schema_min_length.h"
#include "mongo/db/matcher/schema/expression_internal_schema_min_properties.h"
#include "mongo/db/matcher/schema/expression_internal_schema_object_match.h"
#include "mongo/db/matcher/schema/expression_internal_schema_root_doc_eq.h"
#include "mongo/db/matcher/schema/expression_internal_schema_unique_items.h"
#include "mongo/db/matcher/schema/expression_internal_schema_xor.h"
#include "mongo/db/query/index_bounds_builder.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

namespace mongo::stage_builder {
namespace {
/**
 * A struct for storing context across calls to visit() methods in MatchExpressionVisitor's.
 */
struct MatchExpressionVisitorContext {
    MatchExpressionVisitorContext(sbe::value::SlotIdGenerator* slotIdGenerartor,
                                  std::unique_ptr<sbe::PlanStage> inputStage,
                                  sbe::value::SlotId inputVar)
        : slotIdGenerartor{slotIdGenerartor},
          inputStage{std::move(inputStage)},
          inputVar{inputVar} {}

    std::unique_ptr<sbe::PlanStage> done() {
        if (!predicateVars.empty()) {
            invariant(predicateVars.size() == 1);
            inputStage = sbe::makeS<sbe::FilterStage>(
                std::move(inputStage), sbe::makeE<sbe::EVariable>(predicateVars.top()));
            predicateVars.pop();
        }
        return std::move(inputStage);
    }


    sbe::value::SlotIdGenerator* slotIdGenerartor;
    std::unique_ptr<sbe::PlanStage> inputStage;
    std::stack<sbe::value::SlotId> predicateVars;
    std::stack<std::pair<const MatchExpression*, size_t>> nestedLogicalExprs;
    sbe::value::SlotId inputVar;
};

/**
 * A match expression tree walker to be used with MatchExpression visitors in order to translate
 * a MatchExpression tree into an SBE plane stage sub-tree which implements the filter.
 */
class MatchExpressionWalker final {
public:
    MatchExpressionWalker(MatchExpressionConstVisitor* preVisitor,
                          MatchExpressionConstVisitor* inVisitor,
                          MatchExpressionConstVisitor* postVisitor)
        : _preVisitor{preVisitor}, _inVisitor{inVisitor}, _postVisitor{postVisitor} {}

    void preVisit(const MatchExpression* expr) {
        expr->acceptVisitor(_preVisitor);
    }

    void postVisit(const MatchExpression* expr) {
        expr->acceptVisitor(_postVisitor);
    }

    void inVisit(long count, const MatchExpression* expr) {
        expr->acceptVisitor(_inVisitor);
    }

private:
    MatchExpressionConstVisitor* _preVisitor;
    MatchExpressionConstVisitor* _inVisitor;
    MatchExpressionConstVisitor* _postVisitor;
};

/**
 * A helper function to generate a path traversal plan stage at the given nested 'level' of the
 * traversal path. For example, for a dotted path expression {'a.b': 2}, the traversal sub-tree will
 * look like this:
 *
 *     traverse
 *          traversePredicateVar // the global traversal result
 *          elemPredicateVar1 // the result coming from the 'in' branch
 *          fieldVar1 // field 'a' projected in the 'from' branch, this is the field we will be
 *                    // traversing
 *          {traversePredicateVar || elemPredicateVar1} // the folding expression - combing results
 *                                                      // for each element
 *          {traversePredicateVar} // final (early out) expression - when we hit the 'true' value,
 *                                 // we don't have to traverse the whole array
 *      in
 *          project [elemPredicateVar1 = traversePredicateVar]
 *          traverse // nested traversal
 *              traversePredicateVar // the global traversal result
 *              elemPredicateVar2 // the result coming from the 'in' branch
 *              fieldVar2 // field 'b' projected in the 'from' branch, this is the field we will be
 *                        // traversing
 *              {traversePredicateVar || elemPredicateVar2} // the folding expression
 *              {traversePredicateVar} // final (early out) expression
 *          in
 *              project [elemPredicateVar2 = fieldVar2==2] // compare the field 'b' to 2 and store
 *                                                         // the bool result in elemPredicateVar2
 *              limit 1
 *              coscan
 *          from
 *              project [fieldVar2=getField(fieldVar1, 'b')] // project field 'b' from the document
 *                                                           // bound to 'fieldVar1', which is
 *                                                           // field 'a'
 *              limit 1
 *              coscan
 *      from
 *         project [fieldVar1=getField(inputVar, 'a')] // project field 'a' from the document bound
 *                                                     // to 'inputVar'
 *         <inputStage>  // e.g., COLLSCAN
 */
std::unique_ptr<sbe::PlanStage> generateTraverseHelper(MatchExpressionVisitorContext* context,
                                                       std::unique_ptr<sbe::PlanStage> inputStage,
                                                       sbe::value::SlotId inputVar,
                                                       sbe::EPrimBinary::Op op,
                                                       const ComparisonMatchExpression* expr,
                                                       size_t level) {
    using namespace std::literals;

    const auto& path = expr->elementPath().fieldRef();
    invariant(level < path.numParts());

    // The global traversal result.
    const auto& traversePredicateVar = context->predicateVars.top();
    // The field we will be traversing at the current nested level.
    auto fieldVar{context->slotIdGenerartor->generate()};
    // The result coming from the 'in' branch of the traverse plan stage.
    auto elemPredicateVar{context->slotIdGenerartor->generate()};

    // Generate the projection stage to read a sub-field at the current nested level and bind it
    // to 'fieldVar'.
    inputStage = sbe::makeProjectStage(
        std::move(inputStage),
        fieldVar,
        sbe::makeE<sbe::EFunction>(
            "getField"sv,
            sbe::makeEs(sbe::makeE<sbe::EVariable>(inputVar), sbe::makeE<sbe::EConstant>([&]() {
                            auto fieldName = path.getPart(level);
                            return std::string_view{fieldName.rawData(), fieldName.size()};
                        }()))));

    std::unique_ptr<sbe::PlanStage> innerBranch;
    if (level == path.numParts() - 1) {
        // Once the end of the traversal path is reached, evaluate the given predicate and project
        // out the result as 'elemPredicateVar'. The 'rhs' BSON element will contain the value to
        // compare the document field against.
        const auto& rhs = expr->getData();
        auto [tagView, valView] = sbe::bson::convertFrom(
            true, rhs.rawdata(), rhs.rawdata() + rhs.size(), rhs.fieldNameSize() - 1);

        // SBE EConstant assumes the ownership of the value so we have to make a copy here.
        auto [tag, val] = sbe::value::copyValue(tagView, valView);

        innerBranch = sbe::makeProjectStage(
            sbe::makeS<sbe::LimitStage>(sbe::makeS<sbe::CoScanStage>(), 1),
            elemPredicateVar,
            sbe::makeE<sbe::EPrimBinary>(
                op, sbe::makeE<sbe::EVariable>(fieldVar), sbe::makeE<sbe::EConstant>(tag, val)));
    } else {
        // Generate nested traversal.
        innerBranch = sbe::makeProjectStage(
            generateTraverseHelper(context,
                                   sbe::makeS<sbe::LimitStage>(sbe::makeS<sbe::CoScanStage>(), 1),
                                   fieldVar,
                                   op,
                                   expr,
                                   level + 1),
            elemPredicateVar,
            sbe::makeE<sbe::EVariable>(traversePredicateVar));
    }

    // The final traverse stage for the current nested level.
    return sbe::makeS<sbe::TraverseStage>(
        std::move(inputStage),
        std::move(innerBranch),
        fieldVar,
        traversePredicateVar,
        elemPredicateVar,
        sbe::makeE<sbe::EPrimBinary>(sbe::EPrimBinary::logicOr,
                                     sbe::makeE<sbe::EVariable>(traversePredicateVar),
                                     sbe::makeE<sbe::EVariable>(elemPredicateVar)),
        sbe::makeE<sbe::EVariable>(traversePredicateVar));
}

/**
 * For the given MatchExpression 'expr', generates a path traversal SBE plan stage sub-tree
 * implementing the comparison expression.
 */
void generateTraverse(MatchExpressionVisitorContext* context,
                      sbe::EPrimBinary::Op op,
                      const ComparisonMatchExpression* expr) {
    context->predicateVars.push(context->slotIdGenerartor->generate());
    context->inputStage = generateTraverseHelper(
        context, std::move(context->inputStage), context->inputVar, op, expr, 0);

    // If this comparison expression is a branch of a logical $and expression, but not the last
    // one, inject a filter stage to bail out early from the $and predicate without the need to
    // evaluate all branches. If this is the last branch of the $and expression, or if it's not
    // within a logical expression at all, just keep the predicate var on the top on the stack
    // and let the parent expression process it.
    if (!context->nestedLogicalExprs.empty()) {
        if (context->nestedLogicalExprs.top().second > 1 &&
            context->nestedLogicalExprs.top().first->matchType() == MatchExpression::AND) {
            context->inputStage = sbe::makeS<sbe::FilterStage>(
                std::move(context->inputStage),
                sbe::makeE<sbe::EVariable>(context->predicateVars.top()));
            context->predicateVars.pop();
        }
    }
}

/**
 * Generates an SBE plan stage sub-tree implementing a logical $or expression.
 */
void generateLogicalOr(MatchExpressionVisitorContext* context, const OrMatchExpression* expr) {
    invariant(!context->predicateVars.empty());
    invariant(context->predicateVars.size() >= expr->numChildren());

    auto filter = sbe::makeE<sbe::EVariable>(context->predicateVars.top());
    context->predicateVars.pop();

    auto numChildren = expr->numChildren() - 1;
    for (size_t childNum = 0; childNum < numChildren; ++childNum) {
        filter =
            sbe::makeE<sbe::EPrimBinary>(sbe::EPrimBinary::logicOr,
                                         std::move(filter),
                                         sbe::makeE<sbe::EVariable>(context->predicateVars.top()));
        context->predicateVars.pop();
    }

    if (!context->nestedLogicalExprs.empty()) {
        context->predicateVars.push(context->slotIdGenerartor->generate());
        context->inputStage = sbe::makeProjectStage(
            std::move(context->inputStage), context->predicateVars.top(), std::move(filter));
    } else {
        context->inputStage =
            sbe::makeS<sbe::FilterStage>(std::move(context->inputStage), std::move(filter));
    }
}

/**
 * Generates an SBE plan stage sub-tree implementing a logical $and expression.
 */
void generateLogicalAnd(MatchExpressionVisitorContext* context, const AndMatchExpression* expr) {
    auto filter = [&]() {
        if (expr->numChildren() > 0) {
            invariant(!context->predicateVars.empty());
            auto predicateVar = context->predicateVars.top();
            context->predicateVars.pop();
            return sbe::makeE<sbe::EVariable>(predicateVar);
        } else {
            return sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::Boolean, 1);
        }
    }();

    // If this $and expression is a branch of another $and expression, or is a top-level logical
    // expression we can just inject a filter stage without propagating the result of the predicate
    // evaluation to the parent expression, to form a sub-tree of stage->FILTER->stage->FILTER plan
    // stages to support early exit for the $and branches. Otherwise, just project out the result
    // of the predicate evaluation and let the parent expression handle it.
    if (context->nestedLogicalExprs.empty() ||
        context->nestedLogicalExprs.top().first->matchType() == MatchExpression::AND) {
        context->inputStage =
            sbe::makeS<sbe::FilterStage>(std::move(context->inputStage), std::move(filter));
    } else {
        context->predicateVars.push(context->slotIdGenerartor->generate());
        context->inputStage = sbe::makeProjectStage(
            std::move(context->inputStage), context->predicateVars.top(), std::move(filter));
    }
}

/**
 * A match expression pre-visitor used for maintaining nested logical expressions while traversing
 * the match expression tree.
 */
class MatchExpressionPreVisitor final : public MatchExpressionConstVisitor {
public:
    MatchExpressionPreVisitor(MatchExpressionVisitorContext* context) : _context(context) {}

    void visit(const AlwaysFalseMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const AlwaysTrueMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const AndMatchExpression* expr) final {
        _context->nestedLogicalExprs.push({expr, expr->numChildren()});
    }
    void visit(const BitsAllClearMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const BitsAllSetMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const BitsAnyClearMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const BitsAnySetMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const ElemMatchObjectMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const ElemMatchValueMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const EqualityMatchExpression* expr) final {}
    void visit(const ExistsMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const ExprMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const GTEMatchExpression* expr) final {}
    void visit(const GTMatchExpression* expr) final {}
    void visit(const GeoMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const GeoNearMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalExprEqMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaAllElemMatchFromIndexMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaAllowedPropertiesMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaBinDataEncryptedTypeExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaBinDataSubTypeExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaCondMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaEqMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaFmodMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMatchArrayIndexMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMaxItemsMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMaxLengthMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMaxPropertiesMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMinItemsMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMinLengthMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaMinPropertiesMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaObjectMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaRootDocEqMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaTypeExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaUniqueItemsMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const InternalSchemaXorMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const LTEMatchExpression* expr) final {}
    void visit(const LTMatchExpression* expr) final {}
    void visit(const ModMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const NorMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const NotMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const OrMatchExpression* expr) final {
        _context->nestedLogicalExprs.push({expr, expr->numChildren()});
    }
    void visit(const RegexMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const SizeMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const TextMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const TextNoOpMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const TwoDPtInAnnulusExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const TypeMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const WhereMatchExpression* expr) final {
        unsupportedExpression(expr);
    }
    void visit(const WhereNoOpMatchExpression* expr) final {
        unsupportedExpression(expr);
    }

private:
    void unsupportedExpression(const MatchExpression* expr) const {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Match expression is not supported in SBE: "
                                << expr->matchType());
    }

    MatchExpressionVisitorContext* _context;
};

/**
 * A match expression post-visitor which does all the job to translate the match expression tree
 * into an SBE plan stage sub-tree.
 */
class MatchExpressionPostVisitor final : public MatchExpressionConstVisitor {
public:
    MatchExpressionPostVisitor(MatchExpressionVisitorContext* context) : _context(context) {}

    void visit(const AlwaysFalseMatchExpression* expr) final {}
    void visit(const AlwaysTrueMatchExpression* expr) final {}
    void visit(const AndMatchExpression* expr) final {
        _context->nestedLogicalExprs.pop();
        generateLogicalAnd(_context, expr);
    }
    void visit(const BitsAllClearMatchExpression* expr) final {}
    void visit(const BitsAllSetMatchExpression* expr) final {}
    void visit(const BitsAnyClearMatchExpression* expr) final {}
    void visit(const BitsAnySetMatchExpression* expr) final {}
    void visit(const ElemMatchObjectMatchExpression* expr) final {}
    void visit(const ElemMatchValueMatchExpression* expr) final {}
    void visit(const EqualityMatchExpression* expr) final {
        generateTraverse(_context, sbe::EPrimBinary::eq, expr);
    }
    void visit(const ExistsMatchExpression* expr) final {}
    void visit(const ExprMatchExpression* expr) final {}
    void visit(const GTEMatchExpression* expr) final {
        generateTraverse(_context, sbe::EPrimBinary::greaterEq, expr);
    }
    void visit(const GTMatchExpression* expr) final {
        generateTraverse(_context, sbe::EPrimBinary::greater, expr);
    }
    void visit(const GeoMatchExpression* expr) final {}
    void visit(const GeoNearMatchExpression* expr) final {}
    void visit(const InMatchExpression* expr) final {}
    void visit(const InternalExprEqMatchExpression* expr) final {}
    void visit(const InternalSchemaAllElemMatchFromIndexMatchExpression* expr) final {}
    void visit(const InternalSchemaAllowedPropertiesMatchExpression* expr) final {}
    void visit(const InternalSchemaBinDataEncryptedTypeExpression* expr) final {}
    void visit(const InternalSchemaBinDataSubTypeExpression* expr) final {}
    void visit(const InternalSchemaCondMatchExpression* expr) final {}
    void visit(const InternalSchemaEqMatchExpression* expr) final {}
    void visit(const InternalSchemaFmodMatchExpression* expr) final {}
    void visit(const InternalSchemaMatchArrayIndexMatchExpression* expr) final {}
    void visit(const InternalSchemaMaxItemsMatchExpression* expr) final {}
    void visit(const InternalSchemaMaxLengthMatchExpression* expr) final {}
    void visit(const InternalSchemaMaxPropertiesMatchExpression* expr) final {}
    void visit(const InternalSchemaMinItemsMatchExpression* expr) final {}
    void visit(const InternalSchemaMinLengthMatchExpression* expr) final {}
    void visit(const InternalSchemaMinPropertiesMatchExpression* expr) final {}
    void visit(const InternalSchemaObjectMatchExpression* expr) final {}
    void visit(const InternalSchemaRootDocEqMatchExpression* expr) final {}
    void visit(const InternalSchemaTypeExpression* expr) final {}
    void visit(const InternalSchemaUniqueItemsMatchExpression* expr) final {}
    void visit(const InternalSchemaXorMatchExpression* expr) final {}
    void visit(const LTEMatchExpression* expr) final {
        generateTraverse(_context, sbe::EPrimBinary::lessEq, expr);
    }
    void visit(const LTMatchExpression* expr) final {
        generateTraverse(_context, sbe::EPrimBinary::less, expr);
    }
    void visit(const ModMatchExpression* expr) final {}
    void visit(const NorMatchExpression* expr) final {}
    void visit(const NotMatchExpression* expr) final {}
    void visit(const OrMatchExpression* expr) final {
        _context->nestedLogicalExprs.pop();
        generateLogicalOr(_context, expr);
    }
    void visit(const RegexMatchExpression* expr) final {}
    void visit(const SizeMatchExpression* expr) final {}
    void visit(const TextMatchExpression* expr) final {}
    void visit(const TextNoOpMatchExpression* expr) final {}
    void visit(const TwoDPtInAnnulusExpression* expr) final {}
    void visit(const TypeMatchExpression* expr) final {}
    void visit(const WhereMatchExpression* expr) final {}
    void visit(const WhereNoOpMatchExpression* expr) final {}

private:
    MatchExpressionVisitorContext* _context;
};

/**
 * A match expression in-visitor used for maintaining the counter of the processed child expressions
 * of the nested logical expressions in the match expression tree being traversed.
 */
class MatchExpressionInVisitor final : public MatchExpressionConstVisitor {
public:
    MatchExpressionInVisitor(MatchExpressionVisitorContext* context) : _context(context) {}

    void visit(const AlwaysFalseMatchExpression* expr) final {}
    void visit(const AlwaysTrueMatchExpression* expr) final {}
    void visit(const AndMatchExpression* expr) final {
        invariant(_context->nestedLogicalExprs.top().first == expr);
        _context->nestedLogicalExprs.top().second--;
    }
    void visit(const BitsAllClearMatchExpression* expr) final {}
    void visit(const BitsAllSetMatchExpression* expr) final {}
    void visit(const BitsAnyClearMatchExpression* expr) final {}
    void visit(const BitsAnySetMatchExpression* expr) final {}
    void visit(const ElemMatchObjectMatchExpression* expr) final {}
    void visit(const ElemMatchValueMatchExpression* expr) final {}
    void visit(const EqualityMatchExpression* expr) final {}
    void visit(const ExistsMatchExpression* expr) final {}
    void visit(const ExprMatchExpression* expr) final {}
    void visit(const GTEMatchExpression* expr) final {}
    void visit(const GTMatchExpression* expr) final {}
    void visit(const GeoMatchExpression* expr) final {}
    void visit(const GeoNearMatchExpression* expr) final {}
    void visit(const InMatchExpression* expr) final {}
    void visit(const InternalExprEqMatchExpression* expr) final {}
    void visit(const InternalSchemaAllElemMatchFromIndexMatchExpression* expr) final {}
    void visit(const InternalSchemaAllowedPropertiesMatchExpression* expr) final {}
    void visit(const InternalSchemaBinDataEncryptedTypeExpression* expr) final {}
    void visit(const InternalSchemaBinDataSubTypeExpression* expr) final {}
    void visit(const InternalSchemaCondMatchExpression* expr) final {}
    void visit(const InternalSchemaEqMatchExpression* expr) final {}
    void visit(const InternalSchemaFmodMatchExpression* expr) final {}
    void visit(const InternalSchemaMatchArrayIndexMatchExpression* expr) final {}
    void visit(const InternalSchemaMaxItemsMatchExpression* expr) final {}
    void visit(const InternalSchemaMaxLengthMatchExpression* expr) final {}
    void visit(const InternalSchemaMaxPropertiesMatchExpression* expr) final {}
    void visit(const InternalSchemaMinItemsMatchExpression* expr) final {}
    void visit(const InternalSchemaMinLengthMatchExpression* expr) final {}
    void visit(const InternalSchemaMinPropertiesMatchExpression* expr) final {}
    void visit(const InternalSchemaObjectMatchExpression* expr) final {}
    void visit(const InternalSchemaRootDocEqMatchExpression* expr) final {}
    void visit(const InternalSchemaTypeExpression* expr) final {}
    void visit(const InternalSchemaUniqueItemsMatchExpression* expr) final {}
    void visit(const InternalSchemaXorMatchExpression* expr) final {}
    void visit(const LTEMatchExpression* expr) final {}
    void visit(const LTMatchExpression* expr) final {}
    void visit(const ModMatchExpression* expr) final {}
    void visit(const NorMatchExpression* expr) final {}
    void visit(const NotMatchExpression* expr) final {}
    void visit(const OrMatchExpression* expr) final {
        invariant(_context->nestedLogicalExprs.top().first == expr);
        _context->nestedLogicalExprs.top().second--;
    }
    void visit(const RegexMatchExpression* expr) final {}
    void visit(const SizeMatchExpression* expr) final {}
    void visit(const TextMatchExpression* expr) final {}
    void visit(const TextNoOpMatchExpression* expr) final {}
    void visit(const TwoDPtInAnnulusExpression* expr) final {}
    void visit(const TypeMatchExpression* expr) final {}
    void visit(const WhereMatchExpression* expr) final {}
    void visit(const WhereNoOpMatchExpression* expr) final {}

private:
    MatchExpressionVisitorContext* _context;
};

/**
 * Generates an SBE plan stage sub-tree implementing a filter expression represented by the 'root'
 * expression. The 'stage' parameter defines an input stage to the generate SBE plan stage sub-tree.
 * The 'inputVar' defines a variable to read the input document from.
 */
std::unique_ptr<sbe::PlanStage> generateFilter(const MatchExpression* root,
                                               std::unique_ptr<sbe::PlanStage> stage,
                                               sbe::value::SlotIdGenerator* slotIdGenerator,
                                               sbe::value::SlotId inputVar) {
    MatchExpressionVisitorContext context{slotIdGenerator, std::move(stage), inputVar};
    MatchExpressionPreVisitor preVisitor{&context};
    MatchExpressionInVisitor inVisitor{&context};
    MatchExpressionPostVisitor postVisitor{&context};
    MatchExpressionWalker walker{&preVisitor, &inVisitor, &postVisitor};
    tree_walker::walk<true, MatchExpression>(root, &walker);
    return context.done();
}

/**
 * Constructs start/stop key values from the given index 'bounds' and generates SlotId's to bind
 * these keys to.
 */
std::tuple<boost::optional<sbe::value::SlotId>,
           boost::optional<sbe::value::SlotId>,
           std::unique_ptr<KeyString::Value>,
           std::unique_ptr<KeyString::Value>>
makeLowAndHighKeysFromIndexBounds(const IndexBounds& bounds,
                                  bool forward,
                                  const IndexAccessMethod* accessMethod,
                                  sbe::value::SlotIdGenerator* slotIdGenerator) {
    auto startKey{bounds.startKey};
    auto endKey{bounds.endKey};
    auto startKeyInclusive{IndexBounds::isStartIncludedInBound(bounds.boundInclusion)};
    auto endKeyInclusive{IndexBounds::isEndIncludedInBound(bounds.boundInclusion)};

    if (bounds.isSimpleRange ||
        IndexBoundsBuilder::isSingleInterval(
            bounds, &startKey, &startKeyInclusive, &endKey, &endKeyInclusive)) {
        auto lowKey = std::make_unique<KeyString::Value>(
            IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(
                startKey,
                accessMethod->getSortedDataInterface()->getKeyStringVersion(),
                accessMethod->getSortedDataInterface()->getOrdering(),
                forward,
                startKeyInclusive));
        auto highKey = std::make_unique<KeyString::Value>(
            IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(
                endKey,
                accessMethod->getSortedDataInterface()->getKeyStringVersion(),
                accessMethod->getSortedDataInterface()->getOrdering(),
                forward,
                // Use the opposite rule as a normal seek because a forward scan should end after
                // the key if inclusive, and before if exclusive.
                forward != endKeyInclusive));

        return {slotIdGenerator->generate(),
                slotIdGenerator->generate(),
                std::move(lowKey),
                std::move(highKey)};

    } else {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  "Multi-interval index scans not supported in SBE");
    };
}
}  // namespace

// Returns a non-null pointer to the root of a plan tree, or a non-OK status if the PlanStage tree
// could not be constructed.
std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::build(const QuerySolutionNode* root) {
    using namespace std::literals;

    auto stage = [root, this]() -> std::unique_ptr<sbe::PlanStage> {
        switch (root->getType()) {
            case STAGE_COLLSCAN: {
                auto csn = static_cast<const CollectionScanNode*>(root);

                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Tailable collection scans are not supported in SBE",
                        !csn->tailable);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Only forward collection scans are supported in SBE",
                        csn->direction == 1);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Collection scans with shouldTrackLatestOplogTimestamp are not supported "
                        "in SBE",
                        !csn->shouldTrackLatestOplogTimestamp);
                uassert(
                    ErrorCodes::InternalErrorNotSupported,
                    "Collection scans with shouldWaitForOplogVisibility are not supported in SBE",
                    !csn->shouldWaitForOplogVisibility);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Collection scans with minTs are not supported in SBE",
                        !csn->minTs);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Collection scans with maxTs are not supported in SBE",
                        !csn->maxTs);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Collection scans with requestResumeToken are not supported in SBE",
                        !csn->requestResumeToken);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Collection scans with resumeAfterRecordId are not supported in SBE",
                        !csn->resumeAfterRecordId);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Collection scans with stopApplyingFilterAfterFirstMatch are not supported "
                        "in SBE",
                        !csn->stopApplyingFilterAfterFirstMatch);

                _resultSlot = _slotIdGenerator->generate();
                _recordIdSlot = _slotIdGenerator->generate();
                auto stage = sbe::makeS<sbe::ScanStage>(
                    NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
                    _resultSlot,
                    _recordIdSlot,
                    std::vector<std::string>{},
                    std::vector<sbe::value::SlotId>{},
                    boost::none);

                if (root->filter) {
                    stage = generateFilter(
                        root->filter.get(), std::move(stage), _slotIdGenerator.get(), *_resultSlot);
                }

                return stage;
            }
            case STAGE_IXSCAN: {
                auto ixn = static_cast<const IndexScanNode*>(root);

                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Only forward index scans are supported in SBE",
                        ixn->direction == 1);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Index scans with dedup are not supported in SBE",
                        !ixn->shouldDedup);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Index scans with key metadata are not supported in SBE",
                        !ixn->addKeyMetadata);

                auto descriptor = _collection->getIndexCatalog()->findIndexByName(
                    _opCtx, ixn->index.identifier.catalogName);
                auto [lowKeySlot, highKeySlot, lowKey, highKey] = makeLowAndHighKeysFromIndexBounds(
                    ixn->bounds,
                    ixn->direction == 1,
                    _collection->getIndexCatalog()->getEntry(descriptor)->accessMethod(),
                    _slotIdGenerator.get());

                // Scan the index in the range {'lowKeySlot', 'highKeySlot'} (subject to inclusive
                // or exclusive boundaries), and produce a single field recordIdSlot that can be
                // used to position into the collection.
                _recordIdSlot = _slotIdGenerator->generate();
                auto indexScan = sbe::makeS<sbe::IndexScanStage>(
                    NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
                    ixn->index.identifier.catalogName,
                    boost::none,
                    _recordIdSlot,
                    std::vector<std::string>{},
                    std::vector<sbe::value::SlotId>{},
                    lowKeySlot,
                    highKeySlot);
                if (!lowKeySlot && !highKeySlot) {
                    // No seek boundaries have been specified, perform a full index scan.
                    return indexScan;
                } else {
                    invariant(lowKeySlot && highKeySlot);

                    // Construct a constant table scan to deliver a single row with two fields
                    // 'lowKeySlot' and 'highKeySlot', representing seek boundaries, into the
                    // index scan.
                    auto projectKeysStage = sbe::makeProjectStage(
                        sbe::makeS<sbe::LimitStage>(sbe::makeS<sbe::CoScanStage>(), 1),
                        *lowKeySlot,
                        sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::ksValue,
                                                   sbe::value::bitcastFrom(lowKey.release())),
                        *highKeySlot,
                        sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::ksValue,
                                                   sbe::value::bitcastFrom(highKey.release())));

                    // Get the keys from the outer side (guaranteed to see exactly one row) and feed
                    // them to the inner side.
                    return sbe::makeS<sbe::LoopJoinStage>(
                        std::move(projectKeysStage),
                        std::move(indexScan),
                        std::vector<sbe::value::SlotId>{},
                        std::vector<sbe::value::SlotId>{*lowKeySlot, *highKeySlot},
                        nullptr);
                }
            }
            case STAGE_FETCH: {
                auto fn = static_cast<const FetchNode*>(root);
                auto inputStage = build(fn->children[0]);

                uassert(ErrorCodes::InternalError, "RecordId slot is not defined", _recordIdSlot);

                auto recordIdKeySlot = _recordIdSlot;
                _resultSlot = _slotIdGenerator->generate();
                _recordIdSlot = _slotIdGenerator->generate();

                // Scan the collection in the range [recordIdKeySlot, recordIdKeySlot).
                auto collScan = sbe::makeS<sbe::ScanStage>(
                    NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
                    _resultSlot,
                    _recordIdSlot,
                    std::vector<std::string>{},
                    std::vector<sbe::value::SlotId>{},
                    recordIdKeySlot);

                // Get the recordIdKeySlot from the outer side (e.g., IXSCAN) and feed it to the
                // inner side.
                return sbe::makeS<sbe::LoopJoinStage>(
                    std::move(inputStage),
                    std::move(collScan),
                    std::vector<sbe::value::SlotId>{},
                    std::vector<sbe::value::SlotId>{*recordIdKeySlot},
                    nullptr);
            }
            case STAGE_LIMIT: {
                const auto ln = static_cast<const LimitNode*>(root);
                auto inputStage = build(ln->children[0]);
                return std::make_unique<sbe::LimitStage>(std::move(inputStage), ln->limit);
            }
            case STAGE_SORT_KEY_GENERATOR: {
                const auto kn = static_cast<const SortKeyGeneratorNode*>(root);
                // SBE does not use key generator, skip it.
                return build(kn->children[0]);
            }
            case STAGE_SORT: {
                const auto sn = static_cast<const SortNode*>(root);
                uassert(ErrorCodes::InternalErrorNotSupported,
                        "Sort with limit not supported",
                        sn->limit == 0);
                auto sortPattern = SortPattern{sn->pattern, _cq.getExpCtx()};
                auto inputStage = build(sn->children[0]);
                std::vector<sbe::value::SlotId> orderBy;
                for (const auto& part : sortPattern) {
                    uassert(ErrorCodes::InternalErrorNotSupported,
                            "Sorting by expression not supported",
                            !part.expression);
                    uassert(ErrorCodes::InternalErrorNotSupported,
                            "Sorting by dotted paths not supported",
                            part.fieldPath && part.fieldPath->getPathLength() == 1);

                    // slot holding the sort key
                    auto sortFieldVar{_slotIdGenerator->generate()};
                    orderBy.push_back(sortFieldVar);

                    // Generate projection to get the value of the soft key. Ideally, this should be
                    // tracked by a 'reference tracker' at higher level.
                    auto fieldName = part.fieldPath->getFieldName(0);
                    auto fieldNameSV = std::string_view{fieldName.rawData(), fieldName.size()};
                    inputStage = sbe::makeProjectStage(
                        std::move(inputStage),
                        sortFieldVar,
                        sbe::makeE<sbe::EFunction>(
                            "getField"sv,
                            sbe::makeEs(sbe::makeE<sbe::EVariable>(*_resultSlot),
                                        sbe::makeE<sbe::EConstant>(fieldNameSV))));
                }

                std::vector<sbe::value::SlotId> values;
                values.push_back(*_resultSlot);
                if (_recordIdSlot) {
                    values.push_back(*_recordIdSlot);
                }
                return sbe::makeS<sbe::SortStage>(std::move(inputStage), orderBy, values);
            }
            default: {
                str::stream ss;
                ss << "Can't build exec tree for node ";
                root->appendToString(&ss, 0);
                std::string nodeStr(ss);
                uasserted(ErrorCodes::InternalErrorNotSupported, ss);
            }
        }
    }();

    if (root == _solution.root.get()) {
        uassert(ErrorCodes::InternalError, "Result slot is not defined in SBE plan", _resultSlot);

        stage = _recordIdSlot ? sbe::makeProjectStage(std::move(stage),
                                                      sbe::value::SystemSlots::kResultSlot,
                                                      sbe::makeE<sbe::EVariable>(*_resultSlot),
                                                      sbe::value::SystemSlots::kRecordIdSlot,
                                                      sbe::makeE<sbe::EVariable>(*_recordIdSlot))
                              : sbe::makeProjectStage(std::move(stage),
                                                      sbe::value::SystemSlots::kResultSlot,
                                                      sbe::makeE<sbe::EVariable>(*_resultSlot));
    }

    return stage;
}
}  // namespace mongo::stage_builder
