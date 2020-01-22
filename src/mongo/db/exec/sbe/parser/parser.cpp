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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/db/exec/sbe/parser/parser.h"
#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/exchange.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/hash_agg.h"
#include "mongo/db/exec/sbe/stages/hash_join.h"
#include "mongo/db/exec/sbe/stages/ix_scan.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/makeobj.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/exec/sbe/stages/union.h"
#include "mongo/db/exec/sbe/stages/unwind.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
using namespace peg;
using namespace peg::udl;
using namespace std::literals;

static std::string format_error_message(size_t ln, size_t col, const std::string& msg) {
    return str::stream() << ln << ":" << col << ": " << msg << '\n';
}

static constexpr auto syntax = R"(
                ROOT <- OPERATOR
                OPERATOR <- SCAN / PSCAN / SEEK / IXSCAN / IXSEEK / PROJECT / FILTER / CFILTER / MKOBJ / GROUP / HJOIN / NLJOIN / LIMIT / SKIP / COSCAN / TRAVERSE / EXCHANGE / SORT / UNWIND / UNION

                SCAN <- 'scan' IDENT? # optional variable name of the root object (record) delivered by the scan
                               IDENT? # optional variable name of the record id delivered by the scan
                               IDENT_LIST_WITH_RENAMES  # list of projected fields (may be empty)
                               IDENT # collection name to scan

                PSCAN <- 'pscan' IDENT? # optional variable name of the root object (record) delivered by the scan
                                 IDENT? # optional variable name of the record id delivered by the scan
                                 IDENT_LIST_WITH_RENAMES  # list of projected fields (may be empty)
                                 IDENT # collection name to scan

                SEEK <- 'seek' IDENT # variable name of the key
                               IDENT? # optional variable name of the root object (record) delivered by the scan
                               IDENT? # optional variable name of the record id delivered by the scan
                               IDENT_LIST_WITH_RENAMES  # list of projected fields (may be empty)
                               IDENT # collection name to scan

                IXSCAN <- 'ixscan' IDENT? # optional variable name of the root object (record) delivered by the scan
                                   IDENT? # optional variable name of the record id delivered by the scan
                                   IDENT_LIST_WITH_RENAMES  # list of projected fields (may be empty)
                                   IDENT # collection name
                                   IDENT # index name to scan

                IXSEEK <- 'ixseek' IDENT # variable name of the low key
                                   IDENT # variable name of the high key
                                   IDENT? # optional variable name of the root object (record) delivered by the scan
                                   IDENT? # optional variable name of the record id delivered by the scan
                                   IDENT_LIST_WITH_RENAMES  # list of projected fields (may be empty)
                                   IDENT # collection name
                                   IDENT # index name to scan

                PROJECT <- 'project' PROJECT_LIST OPERATOR
                FILTER <- 'filter' '{' EXPR '}' OPERATOR
                CFILTER <- 'cfilter' '{' EXPR '}' OPERATOR
                MKOBJ <- 'mkobj' IDENT (IDENT IDENT_LIST)? IDENT_LIST_WITH_RENAMES OPERATOR
                GROUP <- 'group' IDENT_LIST PROJECT_LIST OPERATOR
                HJOIN <- 'hj' LEFT RIGHT
                LEFT <- 'left' IDENT_LIST IDENT_LIST OPERATOR
                RIGHT <- 'right' IDENT_LIST IDENT_LIST OPERATOR

                NLJOIN <- 'nlj' IDENT_LIST # projected outer variables
                                IDENT_LIST # correlated parameters
                                ('{' EXPR '}')? # optional predicate
                                'left' OPERATOR # outer side
                                'right' OPERATOR # inner side

                LIMIT <- 'limit' NUMBER OPERATOR
                SKIP <- 'skip' NUMBER NUMBER? OPERATOR
                COSCAN <- 'coscan'
                TRAVERSE <- 'traverse' IDENT # output of traverse
                                       IDENT # output of traverse as seen inside the 'in' branch
                                       IDENT # input of traverse
                                       ('{' EXPR '}')? # optional fold expression
                                       ('{' EXPR '}')? # optional final expression
                                       'in' OPERATOR
                                       'from' OPERATOR
                EXCHANGE <- 'exchange' IDENT_LIST NUMBER IDENT OPERATOR
                SORT <- 'sort' IDENT_LIST IDENT_LIST OPERATOR
                UNWIND <- 'unwind' IDENT IDENT IDENT OPERATOR
                UNION <- 'union' IDENT_LIST UNION_BRANCH_LIST

                UNION_BRANCH_LIST <- '[' (UNION_BRANCH (',' UNION_BRANCH)* )?']'
                UNION_BRANCH <- IDENT_LIST OPERATOR

                PROJECT_LIST <- '[' (ASSIGN (',' ASSIGN)* )?']'
                ASSIGN <- IDENT '=' EXPR

                EXPR <- EQOP_EXPR LOG_TOK EXPR / EQOP_EXPR
                LOG_TOK <- <'&&'> / <'||'>

                EQOP_EXPR <- RELOP_EXPR EQ_TOK EQOP_EXPR / RELOP_EXPR
                EQ_TOK <- <'=='> / <'!='>

                RELOP_EXPR <- ADD_EXPR REL_TOK RELOP_EXPR / ADD_EXPR
                REL_TOK <- <'<='> / <'<'> / <'>='> / <'>'>

                ADD_EXPR <- MUL_EXPR ADD_TOK ADD_EXPR / MUL_EXPR
                ADD_TOK <- <'+'> / <'-'>

                MUL_EXPR <- PRIMARY_EXPR  MUL_EXPR / PRIMARY_EXPR
                MUL_TOK <- <'*'> / <'/'>

                PRIMARY_EXPR <- '(' EXPR ')' / CONST_TOK / IF_EXPR / FUN_CALL / IDENT / NUMBER / STRING
                CONST_TOK <- <'true'> / <'false'> / <'null'>

                IF_EXPR <- 'if' '(' EXPR ',' EXPR ',' EXPR ')'

                FUN_CALL <- IDENT '(' (EXPR (',' EXPR)*)? ')'

                IDENT_LIST_WITH_RENAMES <- '[' (IDENT_WITH_RENAME (',' IDENT_WITH_RENAME)*)? ']'
                IDENT_WITH_RENAME <- IDENT ('=' IDENT)?

                IDENT_LIST <- '[' (IDENT (',' IDENT)*)? ']'
                IDENT <- RAW_IDENT/ESC_IDENT

                # string
                STRING <- < '"' (!'"' .)* '"' > / < '\'' (!'\'' .)* '\'' >
                STRING_LIST <- '[' (STRING (',' STRING)*)? ']'

                # number
                NUMBER      <- < [0-9]+ >

                # raw identifier
                RAW_IDENT <- < [$a-zA-Z_] [$a-zA-Z0-9-_]* >

                # anything between quotes
                ESC_IDENT <- < '@' '"' (!'"' .)* '"' >

                %whitespace  <-  ([ \t\r\n]* ('#' (!'\n' .)* '\n' [ \t\r\n]*)*)
                %word        <-  [a-z]+
        )";

void Parser::walkChildren(AstQuery& ast) {
    for (const auto& node : ast.nodes) {
        walk(*node);
    }
}

void Parser::walkIdent(AstQuery& ast) {
    auto str = ast.nodes[0]->token;
    // drop @" .. "
    if (!str.empty() && str[0] == '@') {
        str = str.substr(2, str.size() - 3);
    }
    ast.identifier = std::move(str);
}

void Parser::walkIdentList(AstQuery& ast) {
    walkChildren(ast);
    for (auto& node : ast.nodes) {
        ast.identifiers.emplace_back(std::move(node->identifier));
    }
}

void Parser::walkIdentWithRename(AstQuery& ast) {
    walkChildren(ast);
    std::string identifier;
    std::string rename;

    if (ast.nodes.size() == 1) {
        rename = ast.nodes[0]->identifier;
        identifier = rename;
    } else {
        rename = ast.nodes[0]->identifier;
        identifier = ast.nodes[1]->identifier;
    }

    ast.identifier = std::move(identifier);
    ast.rename = std::move(rename);
}

void Parser::walkIdentListWithRename(AstQuery& ast) {
    walkChildren(ast);

    for (auto& node : ast.nodes) {
        ast.identifiers.emplace_back(std::move(node->identifier));
        ast.renames.emplace_back(std::move(node->rename));
    }
}

void Parser::walkProjectList(AstQuery& ast) {
    walkChildren(ast);

    for (size_t idx = 0; idx < ast.nodes.size(); ++idx) {
        ast.projects[ast.nodes[idx]->identifier] = std::move(ast.nodes[idx]->expr);
    }
}

void Parser::walkAssign(AstQuery& ast) {
    walkChildren(ast);

    ast.identifier = ast.nodes[0]->identifier;
    ast.expr = std::move(ast.nodes[1]->expr);
}

void Parser::walkExpr(AstQuery& ast) {
    walkChildren(ast);
    if (ast.nodes.size() == 1) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else {
        EPrimBinary::Op op;
        if (ast.nodes[1]->token == "&&") {
            op = EPrimBinary::logicAnd;
        }
        if (ast.nodes[1]->token == "||") {
            op = EPrimBinary::logicOr;
        }

        ast.expr =
            makeE<EPrimBinary>(op, std::move(ast.nodes[0]->expr), std::move(ast.nodes[2]->expr));
    }
}

void Parser::walkEqopExpr(AstQuery& ast) {
    walkChildren(ast);
    if (ast.nodes.size() == 1) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else {
        EPrimBinary::Op op;
        if (ast.nodes[1]->token == "==") {
            op = EPrimBinary::eq;
        }
        if (ast.nodes[1]->token == "!=") {
            op = EPrimBinary::neq;
        }

        ast.expr =
            makeE<EPrimBinary>(op, std::move(ast.nodes[0]->expr), std::move(ast.nodes[2]->expr));
    }
}

void Parser::walkRelopExpr(AstQuery& ast) {
    walkChildren(ast);
    if (ast.nodes.size() == 1) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else {
        EPrimBinary::Op op;
        if (ast.nodes[1]->token == "<=") {
            op = EPrimBinary::lessEq;
        }
        if (ast.nodes[1]->token == "<") {
            op = EPrimBinary::less;
        }
        if (ast.nodes[1]->token == ">=") {
            op = EPrimBinary::greaterEq;
        }
        if (ast.nodes[1]->token == ">") {
            op = EPrimBinary::greater;
        }

        ast.expr =
            makeE<EPrimBinary>(op, std::move(ast.nodes[0]->expr), std::move(ast.nodes[2]->expr));
    }
}

void Parser::walkAddExpr(AstQuery& ast) {
    walkChildren(ast);
    if (ast.nodes.size() == 1) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else {
        ast.expr =
            makeE<EPrimBinary>(ast.nodes[1]->token == "+" ? EPrimBinary::add : EPrimBinary::sub,
                               std::move(ast.nodes[0]->expr),
                               std::move(ast.nodes[2]->expr));
    }
}

void Parser::walkMulExpr(AstQuery& ast) {
    walkChildren(ast);
    if (ast.nodes.size() == 1) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else {
        ast.expr =
            makeE<EPrimBinary>(ast.nodes[1]->token == "*" ? EPrimBinary::mul : EPrimBinary::div,
                               std::move(ast.nodes[0]->expr),
                               std::move(ast.nodes[2]->expr));
    }
}

void Parser::walkPrimaryExpr(AstQuery& ast) {
    walkChildren(ast);

    if (ast.nodes[0]->tag == "IDENT"_) {
        ast.expr = std::make_unique<EVariable>(lookupSlotStrict(ast.nodes[0]->identifier));
    } else if (ast.nodes[0]->tag == "NUMBER"_) {
        ast.expr = makeE<EConstant>(value::TypeTags::NumberInt64, std::stoll(ast.nodes[0]->token));
    } else if (ast.nodes[0]->tag == "CONST_TOK"_) {
        if (ast.nodes[0]->token == "true") {
            ast.expr = std::make_unique<EConstant>(value::TypeTags::Boolean, 1);
        } else if (ast.nodes[0]->token == "false") {
            ast.expr = std::make_unique<EConstant>(value::TypeTags::Boolean, 0);
        } else if (ast.nodes[0]->token == "null") {
            ast.expr = std::make_unique<EConstant>(value::TypeTags::Null, 0);
        }
    } else if (ast.nodes[0]->tag == "EXPR"_) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else if (ast.nodes[0]->tag == "IF_EXPR"_) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else if (ast.nodes[0]->tag == "FUN_CALL"_) {
        ast.expr = std::move(ast.nodes[0]->expr);
    } else if (ast.nodes[0]->tag == "STRING"_) {
        std::string str = ast.nodes[0]->token;
        // drop quotes
        str = str.substr(1, str.size() - 2);
        ast.expr = makeE<EConstant>(str);
    }
}
void Parser::walkIfExpr(AstQuery& ast) {
    walkChildren(ast);

    ast.expr = makeE<EIf>(std::move(ast.nodes[0]->expr),
                          std::move(ast.nodes[1]->expr),
                          std::move(ast.nodes[2]->expr));
}

void Parser::walkFunCall(AstQuery& ast) {
    walkChildren(ast);
    std::vector<std::unique_ptr<EExpression>> args;

    for (size_t idx = 1; idx < ast.nodes.size(); ++idx) {
        args.emplace_back(std::move(ast.nodes[idx]->expr));
    }

    ast.expr = makeE<EFunction>(ast.nodes[0]->identifier, std::move(args));
}

void Parser::walkScan(AstQuery& ast) {
    walkChildren(ast);

    std::string recordName;
    std::string recordIdName;
    std::string dbName = _defaultDb;
    std::string collName;
    int projectsPos;

    if (ast.nodes.size() == 4) {
        recordName = std::move(ast.nodes[0]->identifier);
        recordIdName = std::move(ast.nodes[1]->identifier);
        projectsPos = 2;
        collName = std::move(ast.nodes[3]->identifier);
    } else if (ast.nodes.size() == 3) {
        recordName = std::move(ast.nodes[0]->identifier);
        projectsPos = 1;
        collName = std::move(ast.nodes[2]->identifier);
    } else if (ast.nodes.size() == 2) {
        projectsPos = 0;
        collName = std::move(ast.nodes[1]->identifier);
    } else {
        MONGO_UNREACHABLE;
    }

    NamespaceString nssColl{dbName, collName};
    AutoGetCollectionForRead ctxColl(_opCtx, nssColl);
    auto collection = ctxColl.getCollection();
    NamespaceStringOrUUID name =
        collection ? NamespaceStringOrUUID{dbName, collection->uuid()} : nssColl;

    ast.stage = makeS<ScanStage>(name,
                                 lookupSlot(recordName),
                                 lookupSlot(recordIdName),
                                 ast.nodes[projectsPos]->identifiers,
                                 lookupSlots(ast.nodes[projectsPos]->renames),
                                 boost::none);
}

void Parser::walkParallelScan(AstQuery& ast) {
    walkChildren(ast);

    std::string recordName;
    std::string recordIdName;
    std::string dbName = _defaultDb;
    std::string collName;
    int projectsPos;

    if (ast.nodes.size() == 4) {
        recordName = std::move(ast.nodes[0]->identifier);
        recordIdName = std::move(ast.nodes[1]->identifier);
        projectsPos = 2;
        collName = std::move(ast.nodes[3]->identifier);
    } else if (ast.nodes.size() == 3) {
        recordName = std::move(ast.nodes[0]->identifier);
        projectsPos = 1;
        collName = std::move(ast.nodes[2]->identifier);
    } else if (ast.nodes.size() == 2) {
        projectsPos = 0;
        collName = std::move(ast.nodes[1]->identifier);
    } else {
        MONGO_UNREACHABLE;
    }

    NamespaceString nssColl{dbName, collName};
    AutoGetCollectionForRead ctxColl(_opCtx, nssColl);
    auto collection = ctxColl.getCollection();
    NamespaceStringOrUUID name =
        collection ? NamespaceStringOrUUID{dbName, collection->uuid()} : nssColl;

    ast.stage = makeS<ParallelScanStage>(name,
                                         lookupSlot(recordName),
                                         lookupSlot(recordIdName),
                                         ast.nodes[projectsPos]->identifiers,
                                         lookupSlots(ast.nodes[projectsPos]->renames));
}

void Parser::walkSeek(AstQuery& ast) {
    walkChildren(ast);

    std::string recordName;
    std::string recordIdName;
    std::string dbName = _defaultDb;
    std::string collName;
    int projectsPos;

    if (ast.nodes.size() == 5) {
        recordName = std::move(ast.nodes[1]->identifier);
        recordIdName = std::move(ast.nodes[2]->identifier);
        projectsPos = 3;
        collName = std::move(ast.nodes[4]->identifier);
    } else if (ast.nodes.size() == 4) {
        recordName = std::move(ast.nodes[1]->identifier);
        projectsPos = 2;
        collName = std::move(ast.nodes[3]->identifier);
    } else if (ast.nodes.size() == 3) {
        projectsPos = 1;
        collName = std::move(ast.nodes[2]->identifier);
    } else {
        MONGO_UNREACHABLE;
    }

    NamespaceString nssColl{dbName, collName};
    AutoGetCollectionForRead ctxColl(_opCtx, nssColl);
    auto collection = ctxColl.getCollection();
    NamespaceStringOrUUID name =
        collection ? NamespaceStringOrUUID{dbName, collection->uuid()} : nssColl;

    ast.stage = makeS<ScanStage>(name,
                                 lookupSlot(recordName),
                                 lookupSlot(recordIdName),
                                 ast.nodes[projectsPos]->identifiers,
                                 lookupSlots(ast.nodes[projectsPos]->renames),
                                 lookupSlot(ast.nodes[0]->identifier));
}
void Parser::walkIndexScan(AstQuery& ast) {
    walkChildren(ast);

    std::string recordName;
    std::string recordIdName;
    std::string dbName = _defaultDb;
    std::string collName;
    std::string indexName;
    int projectsPos;

    if (ast.nodes.size() == 5) {
        recordName = std::move(ast.nodes[0]->identifier);
        recordIdName = std::move(ast.nodes[1]->identifier);
        projectsPos = 2;
        collName = std::move(ast.nodes[3]->identifier);
        indexName = std::move(ast.nodes[4]->identifier);
    } else if (ast.nodes.size() == 4) {
        recordName = std::move(ast.nodes[0]->identifier);
        projectsPos = 1;
        collName = std::move(ast.nodes[2]->identifier);
        indexName = std::move(ast.nodes[3]->identifier);
    } else if (ast.nodes.size() == 3) {
        projectsPos = 0;
        collName = std::move(ast.nodes[1]->identifier);
        indexName = std::move(ast.nodes[2]->identifier);
    } else {
        MONGO_UNREACHABLE;
    }

    NamespaceString nssColl{dbName, collName};
    AutoGetCollectionForRead ctxColl(_opCtx, nssColl);
    auto collection = ctxColl.getCollection();
    NamespaceStringOrUUID name =
        collection ? NamespaceStringOrUUID{dbName, collection->uuid()} : nssColl;

    ast.stage = makeS<IndexScanStage>(name,
                                      indexName,
                                      lookupSlot(recordName),
                                      lookupSlot(recordIdName),
                                      ast.nodes[projectsPos]->identifiers,
                                      lookupSlots(ast.nodes[projectsPos]->renames),
                                      boost::none,
                                      boost::none);
}

void Parser::walkIndexSeek(AstQuery& ast) {
    walkChildren(ast);

    std::string recordName;
    std::string recordIdName;
    std::string dbName = _defaultDb;
    std::string collName;
    std::string indexName;
    int projectsPos;

    if (ast.nodes.size() == 7) {
        recordName = std::move(ast.nodes[2]->identifier);
        recordIdName = std::move(ast.nodes[3]->identifier);
        projectsPos = 4;
        collName = std::move(ast.nodes[5]->identifier);
        indexName = std::move(ast.nodes[6]->identifier);
    } else if (ast.nodes.size() == 6) {
        recordName = std::move(ast.nodes[2]->identifier);
        projectsPos = 3;
        collName = std::move(ast.nodes[4]->identifier);
        indexName = std::move(ast.nodes[5]->identifier);
    } else if (ast.nodes.size() == 5) {
        projectsPos = 2;
        collName = std::move(ast.nodes[3]->identifier);
        indexName = std::move(ast.nodes[4]->identifier);
    } else {
        MONGO_UNREACHABLE;
    }

    NamespaceString nssColl{dbName, collName};
    AutoGetCollectionForRead ctxColl(_opCtx, nssColl);
    auto collection = ctxColl.getCollection();
    NamespaceStringOrUUID name =
        collection ? NamespaceStringOrUUID{dbName, collection->uuid()} : nssColl;

    ast.stage = makeS<IndexScanStage>(name,
                                      indexName,
                                      lookupSlot(recordName),
                                      lookupSlot(recordIdName),
                                      ast.nodes[projectsPos]->identifiers,
                                      lookupSlots(ast.nodes[projectsPos]->renames),
                                      lookupSlot(ast.nodes[0]->identifier),
                                      lookupSlot(ast.nodes[1]->identifier));
}
void Parser::walkProject(AstQuery& ast) {
    walkChildren(ast);

    ast.stage = makeS<ProjectStage>(std::move(ast.nodes[1]->stage),
                                    lookupSlots(std::move(ast.nodes[0]->projects)));
}

void Parser::walkFilter(AstQuery& ast) {
    walkChildren(ast);

    ast.stage =
        makeS<FilterStage<false>>(std::move(ast.nodes[1]->stage), std::move(ast.nodes[0]->expr));
}

void Parser::walkCFilter(AstQuery& ast) {
    walkChildren(ast);

    ast.stage =
        makeS<FilterStage<true>>(std::move(ast.nodes[1]->stage), std::move(ast.nodes[0]->expr));
}

void Parser::walkSort(AstQuery& ast) {
    walkChildren(ast);

    // TODO parse asc/desc
    std::vector<value::SortDirection> dirs(ast.nodes[0]->identifiers.size(),
                                           value::SortDirection::Ascending);

    ast.stage = makeS<SortStage>(std::move(ast.nodes[2]->stage),
                                 lookupSlots(ast.nodes[0]->identifiers),
                                 dirs,
                                 lookupSlots(ast.nodes[1]->identifiers),
                                 std::numeric_limits<std::size_t>::max());
}

void Parser::walkUnion(AstQuery& ast) {
    walkChildren(ast);

    std::vector<std::unique_ptr<PlanStage>> inputStages;
    std::vector<std::vector<value::SlotId>> inputVals;
    std::vector<value::SlotId> outputVals{lookupSlots(ast.nodes[0]->identifiers)};

    for (size_t idx = 0; idx < ast.nodes[1]->nodes.size(); idx++) {
        inputVals.push_back(lookupSlots(ast.nodes[1]->nodes[idx]->identifiers));
        inputStages.push_back(std::move(ast.nodes[1]->nodes[idx]->stage));
    }

    uassert(ErrorCodes::BadValue,
            "Union output values and input values mismatch",
            std::all_of(
                inputVals.begin(), inputVals.end(), [size = outputVals.size()](const auto& slots) {
                    return slots.size() == size;
                }));

    ast.stage = makeS<UnionStage>(std::move(inputStages), inputVals, outputVals);
}

void Parser::walkUnionBranch(AstQuery& ast) {
    walkChildren(ast);

    ast.identifiers = std::move(ast.nodes[0]->identifiers);
    ast.stage = std::move(ast.nodes[1]->stage);
}

void Parser::walkUnwind(AstQuery& ast) {
    walkChildren(ast);

    ast.stage = makeS<UnwindStage>(std::move(ast.nodes[3]->stage),
                                   lookupSlotStrict(ast.nodes[2]->identifier),
                                   lookupSlotStrict(ast.nodes[0]->identifier),
                                   lookupSlotStrict(ast.nodes[1]->identifier));
}

void Parser::walkMkObj(AstQuery& ast) {
    walkChildren(ast);

    std::string newRootName = ast.nodes[0]->identifier;
    std::string oldRootName;
    std::vector<std::string> restrictFields;

    size_t projectListPos;
    size_t inputPos;
    if (ast.nodes.size() == 3) {
        projectListPos = 1;
        inputPos = 2;
    } else {
        oldRootName = ast.nodes[1]->identifier;
        restrictFields = std::move(ast.nodes[2]->identifiers);
        projectListPos = 3;
        inputPos = 4;
    }

    ast.stage = makeS<MakeObjStage>(std::move(ast.nodes[inputPos]->stage),
                                    lookupSlotStrict(newRootName),
                                    lookupSlot(oldRootName),
                                    std::move(restrictFields),
                                    std::move(ast.nodes[projectListPos]->renames),
                                    lookupSlots(std::move(ast.nodes[projectListPos]->identifiers)));
}

void Parser::walkGroup(AstQuery& ast) {
    walkChildren(ast);

    ast.stage = makeS<HashAggStage>(std::move(ast.nodes[2]->stage),
                                    lookupSlots(std::move(ast.nodes[0]->identifiers)),
                                    lookupSlots(std::move(ast.nodes[1]->projects)));
}

void Parser::walkHashJoin(AstQuery& ast) {
    walkChildren(ast);
    ast.stage =
        makeS<HashJoinStage>(std::move(ast.nodes[0]->nodes[2]->stage),          // outer
                             std::move(ast.nodes[1]->nodes[2]->stage),          // inner
                             lookupSlots(ast.nodes[0]->nodes[0]->identifiers),  // outer conditions
                             lookupSlots(ast.nodes[0]->nodes[1]->identifiers),  // outer projections
                             lookupSlots(ast.nodes[1]->nodes[0]->identifiers),  // inner conditions
                             lookupSlots(ast.nodes[1]->nodes[1]->identifiers)   // inner projections
        );
}

void Parser::walkNLJoin(AstQuery& ast) {
    walkChildren(ast);
    size_t outerPos;
    size_t innerPos;
    std::unique_ptr<EExpression> predicate;

    if (ast.nodes.size() == 5) {
        predicate = std::move(ast.nodes[2]->expr);
        outerPos = 3;
        innerPos = 4;
    } else {
        outerPos = 2;
        innerPos = 3;
    }

    ast.stage = makeS<LoopJoinStage>(std::move(ast.nodes[outerPos]->stage),
                                     std::move(ast.nodes[innerPos]->stage),
                                     lookupSlots(ast.nodes[0]->identifiers),
                                     lookupSlots(ast.nodes[1]->identifiers),
                                     std::move(predicate));
}

void Parser::walkLimit(AstQuery& ast) {
    walkChildren(ast);

    ast.stage = makeS<LimitSkipStage>(
        std::move(ast.nodes[1]->stage), std::stoi(ast.nodes[0]->token), boost::none);
}

void Parser::walkSkip(AstQuery& ast) {
    walkChildren(ast);

    if (ast.nodes.size() == 3) {
        ast.stage = makeS<LimitSkipStage>(std::move(ast.nodes[2]->stage),
                                          std::stoi(ast.nodes[1]->token),
                                          std::stoi(ast.nodes[0]->token));
    } else {
        ast.stage = makeS<LimitSkipStage>(
            std::move(ast.nodes[1]->stage), boost::none, std::stoi(ast.nodes[0]->token));
    }
}

void Parser::walkCoScan(AstQuery& ast) {
    walkChildren(ast);

    ast.stage = makeS<CoScanStage>();
}

void Parser::walkTraverse(AstQuery& ast) {
    walkChildren(ast);
    size_t inPos;
    size_t fromPos;
    size_t foldPos = 0;
    size_t finalPos = 0;

    if (ast.nodes.size() == 5) {
        inPos = 3;
        fromPos = 4;
    } else if (ast.nodes.size() == 6) {
        foldPos = 3;
        inPos = 4;
        fromPos = 5;
    } else {
        foldPos = 3;
        finalPos = 4;
        inPos = 5;
        fromPos = 6;
    }
    ast.stage = makeS<TraverseStage>(std::move(ast.nodes[fromPos]->stage),
                                     std::move(ast.nodes[inPos]->stage),
                                     lookupSlotStrict(ast.nodes[2]->identifier),
                                     lookupSlotStrict(ast.nodes[0]->identifier),
                                     lookupSlotStrict(ast.nodes[1]->identifier),
                                     foldPos ? std::move(ast.nodes[foldPos]->expr) : nullptr,
                                     finalPos ? std::move(ast.nodes[finalPos]->expr) : nullptr);
}

void Parser::walkExchange(AstQuery& ast) {
    walkChildren(ast);
    ExchangePolicy policy = [&ast] {
        if (ast.nodes[2]->identifier == "round") {
            return ExchangePolicy::roundrobin;
        }

        if (ast.nodes[2]->identifier == "bcast") {
            return ExchangePolicy::broadcast;
        }
        uasserted(ErrorCodes::BadValue, "unknown exchange policy");
    }();
    ast.stage = makeS<ExchangeConsumer>(std::move(ast.nodes[3]->stage),
                                        std::stoll(ast.nodes[1]->token),
                                        lookupSlots(ast.nodes[0]->identifiers),
                                        policy,
                                        nullptr,
                                        nullptr);
}

void Parser::walk(AstQuery& ast) {
    switch (ast.tag) {
        case "OPERATOR"_:
            walkChildren(ast);
            ast.stage = std::move(ast.nodes[0]->stage);
            break;
        case "ROOT"_:
            walkChildren(ast);
            ast.stage = std::move(ast.nodes[0]->stage);
            break;
        case "SCAN"_:
            walkScan(ast);
            break;
        case "PSCAN"_:
            walkParallelScan(ast);
            break;
        case "SEEK"_:
            walkSeek(ast);
            break;
        case "IXSCAN"_:
            walkIndexScan(ast);
            break;
        case "IXSEEK"_:
            walkIndexSeek(ast);
            break;
        case "PROJECT"_:
            walkProject(ast);
            break;
        case "FILTER"_:
            walkFilter(ast);
            break;
        case "CFILTER"_:
            walkCFilter(ast);
            break;
        case "SORT"_:
            walkSort(ast);
            break;
        case "UNION"_:
            walkUnion(ast);
            break;
        case "UNION_BRANCH_LIST"_:
            walkChildren(ast);
            break;
        case "UNION_BRANCH"_:
            walkUnionBranch(ast);
            break;
        case "UNWIND"_:
            walkUnwind(ast);
            break;
        case "MKOBJ"_:
            walkMkObj(ast);
            break;
        case "GROUP"_:
            walkGroup(ast);
            break;
        case "HJOIN"_:
            walkHashJoin(ast);
            break;
        case "NLJOIN"_:
            walkNLJoin(ast);
            break;
        case "LIMIT"_:
            walkLimit(ast);
            break;
        case "SKIP"_:
            walkSkip(ast);
            break;
        case "COSCAN"_:
            walkCoScan(ast);
            break;
        case "TRAVERSE"_:
            walkTraverse(ast);
            break;
        case "EXCHANGE"_:
            walkExchange(ast);
            break;
        case "IDENT"_:
            walkIdent(ast);
            break;
        case "IDENT_LIST"_:
            walkIdentList(ast);
            break;
        case "IDENT_WITH_RENAME"_:
            walkIdentWithRename(ast);
            break;
        case "IDENT_LIST_WITH_RENAMES"_:
            walkIdentListWithRename(ast);
            break;
        case "PROJECT_LIST"_:
            walkProjectList(ast);
            break;
        case "ASSIGN"_:
            walkAssign(ast);
            break;
        case "EXPR"_:
            walkExpr(ast);
            break;
        case "EQOP_EXPR"_:
            walkEqopExpr(ast);
            break;
        case "RELOP_EXPR"_:
            walkRelopExpr(ast);
            break;
        case "ADD_EXPR"_:
            walkAddExpr(ast);
            break;
        case "MUL_EXPR"_:
            walkMulExpr(ast);
            break;
        case "PRIMARY_EXPR"_:
            walkPrimaryExpr(ast);
            break;
        case "IF_EXPR"_:
            walkIfExpr(ast);
            break;
        case "FUN_CALL"_:
            walkFunCall(ast);
            break;
        default:
            walkChildren(ast);
    }
}

Parser::Parser() {
    _parser.log = [&](size_t ln, size_t col, const std::string& msg) {
        LOG(0) << format_error_message(ln, col, msg);
    };

    if (!_parser.load_grammar(syntax)) {
        uasserted(ErrorCodes::InternalError, "Invalid syntax definition.");
    }

    _parser.enable_packrat_parsing();

    _parser.enable_ast<AstQuery>();
}

std::unique_ptr<PlanStage> Parser::parse(OperationContext* opCtx,
                                         std::string_view defaultDb,
                                         std::string_view line) {
    std::shared_ptr<AstQuery> ast;

    _opCtx = opCtx;
    _defaultDb = defaultDb;

    auto result = _parser.parse_n(line.data(), line.size(), ast);
    uassert(ErrorCodes::FailedToParse, "Syntax error in query.", result);

    walk(*ast);
    uassert(ErrorCodes::FailedToParse, "Query does not have the root.", ast->stage);

    return std::move(ast->stage);
}
}  // namespace sbe
}  // namespace mongo
