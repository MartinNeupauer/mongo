/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "query_analysis.h"

#include <stack>

#include "encryption_schema_tree.h"
#include "encryption_update_visitor.h"
#include "fle_match_expression.h"
#include "fle_pipeline.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/expression_type.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/ops/parsed_update.h"
#include "mongo/db/ops/write_ops.h"
#include "mongo/db/ops/write_ops_gen.h"
#include "mongo/db/pipeline/stub_mongo_process_interface.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/count_command_gen.h"
#include "mongo/db/query/distinct_command_gen.h"
#include "mongo/db/query/find_and_modify_request.h"
#include "mongo/db/query/query_request.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/rpc/op_msg.h"

namespace mongo {

namespace {

static constexpr auto kJsonSchema = "jsonSchema"_sd;
static constexpr auto kHasEncryptionPlaceholders = "hasEncryptionPlaceholders"_sd;
static constexpr auto kSchemaRequiresEncryption = "schemaRequiresEncryption"_sd;
static constexpr auto kResult = "result"_sd;

std::string typeSetToString(const MatcherTypeSet& typeSet) {
    StringBuilder sb;
    sb << "[ ";
    if (typeSet.allNumbers) {
        sb << "number ";
    }
    for (auto&& type : typeSet.bsonTypes) {
        sb << typeName(type) << " ";
    }
    sb << "]";
    return sb.str();
}

/**
 * Extracts and returns the jsonSchema field in the command 'obj'. Populates 'stripped' with the
 * same fields as 'obj', except without the jsonSchema.
 *
 * Throws an AssertionException if the schema is missing or not of the correct type.
 */
BSONObj extractJSONSchema(BSONObj obj, BSONObjBuilder* stripped) {
    boost::optional<BSONObj> ret;
    for (auto& e : obj) {
        if (e.fieldNameStringData() == kJsonSchema) {
            uassert(51090, "jsonSchema is expected to be a object", e.type() == Object);

            ret = e.Obj();
        } else {
            stripped->append(e);
        }
    }

    uassert(51073, "jsonSchema is a required command field", ret);

    stripped->doneFast();

    return ret.get();
}

/**
 * If 'collation' is boost::none, returns nullptr. Otherwise, uses the collator factory decorating
 * 'opCtx' to produce a collator for 'collation'.
 */
std::unique_ptr<CollatorInterface> parseCollator(OperationContext* opCtx,
                                                 const boost::optional<BSONObj>& collation) {
    if (!collation) {
        return nullptr;
    }

    auto collatorFactory = CollatorFactoryInterface::get(opCtx->getServiceContext());
    return uassertStatusOK(collatorFactory->makeFromBSON(*collation));
}

/**
 * If the 'collation' field exists in 'command', extracts it and returns the corresponding
 * CollatorInterface pointer. Otherwise, returns nullptr.
 *
 * Throws a user assertion if the 'collation' field exists in 'command' but is not of BSON type
 * Object.
 */
std::unique_ptr<CollatorInterface> extractCollator(OperationContext* opCtx,
                                                   const BSONObj& command) {
    auto collationElt = command["collation"_sd];
    if (!collationElt) {
        return nullptr;
    }

    uassert(31084,
            "collation command parameter must be of type Object",
            collationElt.type() == BSONType::Object);
    return parseCollator(opCtx, collationElt.embeddedObject());
}

/**
 * Recursively descends through the doc curDoc. For each key in the doc, checks the schema and
 * replaces it with an encryption placeholder if neccessary. If origDoc is given it is passed to
 * buildEncryptPlaceholder() to resolve JSON Pointers. If it is not given, schemas with pointers
 * will error.
 *
 * Does not descend into arrays.
 */
BSONObj replaceEncryptedFieldsRecursive(const EncryptionSchemaTreeNode* schema,
                                        BSONObj curDoc,
                                        EncryptionPlaceholderContext placeholderContext,
                                        const boost::optional<BSONObj>& origDoc,
                                        const CollatorInterface* collator,
                                        FieldRef* leadingPath,
                                        bool* encryptedFieldFound) {
    BSONObjBuilder builder;
    for (auto&& element : curDoc) {
        auto fieldName = element.fieldNameStringData();
        leadingPath->appendPart(fieldName);
        if (auto metadata = schema->getEncryptionMetadataForPath(*leadingPath)) {
            *encryptedFieldFound = true;
            BSONObj placeholder = buildEncryptPlaceholder(
                element, metadata.get(), placeholderContext, collator, origDoc, *schema);
            builder.append(placeholder[fieldName]);
        } else if (element.type() == BSONType::Object) {
            builder.append(fieldName,
                           replaceEncryptedFieldsRecursive(schema,
                                                           element.embeddedObject(),
                                                           placeholderContext,
                                                           origDoc,
                                                           collator,
                                                           leadingPath,
                                                           encryptedFieldFound));
        } else if (element.type() == BSONType::Array) {
            // Encrypting beneath an array is not supported. If the user has an array along an
            // encrypted path, they have violated the type:"object" condition of the schema. For
            // example, if the user's schema indicates that "foo.bar" is encrypted, it is implied
            // that "foo" is an object. We should therefore return an error if the user attempts to
            // insert a document such as {foo: [{bar: 1}, {bar: 2}]}.
            //
            // Although mongocryptd doesn't enforce the provided JSON Schema in full, we make an
            // effort here to enforce the encryption-related aspects of the schema. Ensuring that
            // there are no arrays along an encrypted path falls within this mandate.
            uassert(31006,
                    str::stream() << "An array at path '" << leadingPath->dottedField()
                                  << "' would violate the schema",
                    !schema->containsEncryptedNodeBelowPrefix(*leadingPath));
            builder.append(element);
        } else {
            builder.append(element);
        }

        leadingPath->removeLastPart();
    }
    return builder.obj();
}

/**
 * Returns a new BSONObj that has all of the fields from 'response' that are also in 'original'
 * and always removes $db.
 */
BSONObj removeExtraFields(const std::set<StringData>& original, const BSONObj& response) {
    BSONObjBuilder bob;
    for (auto&& elem : response) {
        auto field = elem.fieldNameStringData();
        // "$db" is always removed because the drivers adds it to every message sent. If
        // we sent them in the reply, the drivers would wind up trying to add it again when
        // sending the query to mongod. In addition, some queries may not want to send $db at all
        // to a specific mongod version, so we remove it here instead of in the driver.
        if (field == "$db") {
            continue;
        }
        if (original.find(field) != original.end()) {
            bob.append(elem);
        }
    }
    return bob.obj();
}

/**
 * Parses the MatchExpression given by 'filter' and replaces encrypted fields with their appropriate
 * EncryptionPlaceholder according to the schema.
 *
 * Returns a PlaceHolderResult containing the new MatchExpression and a boolean to indicate whether
 * it contains an intent-to-encrypt marking. Throws an assertion if the MatchExpression is invalid
 * or contains an invalid operator over an encrypted field.
 *
 * Throws an assertion if 'filter' might require a collation-aware comparison and 'collator' is
 * non-null.
 */
PlaceHolderResult replaceEncryptedFieldsInFilter(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const EncryptionSchemaTreeNode& schemaTree,
    BSONObj filter) {
    // Build a parsed MatchExpression from the filter, allowing all special features.
    auto matchExpr = uassertStatusOK(MatchExpressionParser::parse(
        filter, expCtx, ExtensionsCallbackNoop(), MatchExpressionParser::kAllowAllSpecialFeatures));

    // Build a FLEMatchExpression, which will replace encrypted values with their appropriate
    // intent-to-encrypt markings.
    FLEMatchExpression fleMatchExpr(std::move(matchExpr), schemaTree);

    // Replace the previous filter object with the new MatchExpression after marking it for
    // encryption.
    BSONObjBuilder bob;
    fleMatchExpr.getMatchExpression()->serialize(&bob);

    return {fleMatchExpr.containsEncryptedPlaceholders(),
            schemaTree.containsEncryptedNode(),
            bob.obj()};
}

/**
 * Parses the update object given by 'update' and replaces encrypted fields with their appropriate
 * EncryptionPlaceholder according to the schema.
 *
 * Returns a PlaceHolderResult containing the new update object and a boolean to indicate whether
 * it contains an intent-to-encrypt marking. Throws an assertion if the update object is invalid
 * or contains an invalid operator over an encrypted field.
 */
PlaceHolderResult replaceEncryptedFieldsInUpdate(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const EncryptionSchemaTreeNode& schemaTree,
    BSONObj update,
    const std::vector<mongo::BSONObj>& arrayFilters) {
    UpdateDriver driver(expCtx);
    // Although arrayFilters cannot contain encrypted fields, pass them through to the UpdateDriver
    // to prevent parsing errors for arrayFilters on a non-encrypted field path.
    auto parsedArrayFilters = uassertStatusOK(ParsedUpdate::parseArrayFilters(
        arrayFilters, expCtx->opCtx, const_cast<CollatorInterface*>(expCtx->getCollator())));
    driver.parse(update, parsedArrayFilters);

    // 'updateVisitor' must live through driver serialization.
    auto updateVisitor = EncryptionUpdateVisitor(schemaTree);

    bool hasEncryptionPlaceholder = false;
    switch (driver.type()) {
        case UpdateDriver::UpdateType::kOperator:
            driver.visitRoot(&updateVisitor);
            hasEncryptionPlaceholder = updateVisitor.hasPlaceholder();
            break;
        case UpdateDriver::UpdateType::kReplacement: {
            auto updateExec = static_cast<ObjectReplaceExecutor*>(driver.getUpdateExecutor());
            // Replacement update need not respect the collation. It is legal to use replacement
            // update to create an encrypted string field, even if the update operation has a
            // non-simple collation.
            const CollatorInterface* collator = nullptr;
            auto placeholder = replaceEncryptedFields(updateExec->getReplacement(),
                                                      &schemaTree,
                                                      EncryptionPlaceholderContext::kWrite,
                                                      FieldRef{},
                                                      boost::none,
                                                      collator);
            if (placeholder.hasEncryptionPlaceholders) {
                updateExec->setReplacement(placeholder.result);
                hasEncryptionPlaceholder = true;
            }
            break;
        }
        case UpdateDriver::UpdateType::kPipeline:
            MONGO_UNREACHABLE;
    }

    return PlaceHolderResult{hasEncryptionPlaceholder,
                             schemaTree.containsEncryptedNode(),
                             driver.serialize().getDocument().toBson()};
}

PlaceHolderResult addPlaceHoldersForFind(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                         const std::string& dbName,
                                         const BSONObj& cmdObj,
                                         std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // Parse to a QueryRequest to ensure the command syntax is valid. We can use a temporary
    // database name however the collection name will be used when serializing back to BSON.
    auto qr = uassertStatusOK(QueryRequest::makeFromFindCommand(
        CommandHelpers::parseNsCollectionRequired(dbName, cmdObj), cmdObj, false));

    auto placeholder = replaceEncryptedFieldsInFilter(expCtx, *schemaTree, qr->getFilter());

    BSONObjBuilder bob;
    for (auto&& elem : cmdObj) {
        if (elem.fieldNameStringData() == "filter") {
            BSONObjBuilder filterBob = bob.subobjStart("filter");
            filterBob.appendElements(placeholder.result);
        } else {
            bob.append(elem);
        }
    }
    return {placeholder.hasEncryptionPlaceholders, placeholder.schemaRequiresEncryption, bob.obj()};
}

PlaceHolderResult addPlaceHoldersForAggregate(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const std::string& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    uassert(ErrorCodes::NotImplemented,
            "Aggregate command is not supported with encryption",
            getTestCommandsEnabled());
    // Parse the command to an AggregationRequest to verify that there no unknown fields.
    auto request = uassertStatusOK(AggregationRequest::parseFromBSON(dbName, cmdObj, boost::none));

    // Add the populated list of involved namespaces to the expression context, needed at parse time
    // by stages such as $lookup and $out.
    expCtx->ns = request.getNamespaceString();
    expCtx->setResolvedNamespaces([&]() {
        const LiteParsedPipeline liteParsedPipeline(request);
        const auto& pipelineInvolvedNamespaces = liteParsedPipeline.getInvolvedNamespaces();

        StringMap<ExpressionContext::ResolvedNamespace> resolvedNamespaces;
        for (auto&& involvedNs : pipelineInvolvedNamespaces) {
            resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
        }
        return resolvedNamespaces;
    }());

    // Build a FLEPipeline which will replace encrypted fields with intent-to-encrypt markings, then
    // update the AggregationRequest with the new pipeline if there were any replaced fields.
    FLEPipeline flePipe{uassertStatusOK(Pipeline::parse(request.getPipeline(), expCtx)),
                        *schemaTree.get()};

    // Serialize the translated command by manually appending each field that was present in the
    // original command, replacing the pipeline with the translated version containing
    // intent-to-encrypt markings.
    BSONObjBuilder bob;
    for (auto&& elem : cmdObj) {
        if (elem.fieldNameStringData() == "pipeline"_sd) {
            BSONArrayBuilder arr(bob.subarrayStart("pipeline"));
            for (auto&& stage : flePipe.getPipeline().serialize()) {
                invariant(stage.getType() == BSONType::Object);
                arr.append(stage.getDocument().toBson());
            }
        } else {
            bob.append(elem);
        }
    }

    return {flePipe.hasEncryptedPlaceholders, schemaTree->containsEncryptedNode(), bob.obj()};
}

PlaceHolderResult addPlaceHoldersForCount(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                          const std::string& dbName,
                                          const BSONObj& cmdObj,
                                          std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    BSONObjBuilder resultBuilder;
    auto countCmd = CountCommand::parse(IDLParserErrorContext("count"), cmdObj);
    auto query = countCmd.getQuery();

    auto newQueryPlaceholder = replaceEncryptedFieldsInFilter(expCtx, *schemaTree, query);
    countCmd.setQuery(newQueryPlaceholder.result);

    return PlaceHolderResult{newQueryPlaceholder.hasEncryptionPlaceholders,
                             newQueryPlaceholder.schemaRequiresEncryption ||
                                 schemaTree->containsEncryptedNode(),
                             countCmd.toBSON(cmdObj)};
}

PlaceHolderResult addPlaceHoldersForDistinct(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                             const std::string& dbName,
                                             const BSONObj& cmdObj,
                                             std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto parsedDistinct = DistinctCommand::parse(IDLParserErrorContext("distinct"), cmdObj);

    if (auto keyMetadata =
            schemaTree->getEncryptionMetadataForPath(FieldRef(parsedDistinct.getKey()))) {
        uassert(51131,
                "The distinct key is not allowed to be marked for encryption with a non-UUID keyId",
                keyMetadata->keyId.type() != EncryptSchemaKeyId::Type::kJSONPointer);
        uassert(31026,
                "Distinct key is not allowed to be marked for encryption with the randomized "
                "encryption algorithm",
                keyMetadata->algorithm != FleAlgorithmEnum::kRandom);

        // TODO SERVER-40798: Relax this check if the schema indicates that the encrypted distinct
        // key is not a string.
        uassert(31058,
                "Distinct key cannot be an encrypted field if the collation is non-simple",
                !expCtx->getCollator());
    } else {
        uassert(31027,
                "Distinct key is not allowed to be a prefix of an encrypted field",
                !schemaTree->containsEncryptedNodeBelowPrefix(FieldRef(parsedDistinct.getKey())));
    }

    PlaceHolderResult placeholder;
    if (auto query = parsedDistinct.getQuery()) {
        // Replace any encrypted fields in the query, and overwrite the original query from the
        // parsed command.
        placeholder =
            replaceEncryptedFieldsInFilter(expCtx, *schemaTree, parsedDistinct.getQuery().get());
        parsedDistinct.setQuery(placeholder.result);
    }

    // Serialize the parsed distinct command. Passing the original command object to 'serialize()'
    // allows the IDL to merge generic fields which the command does not specifically handle.
    return PlaceHolderResult{placeholder.hasEncryptionPlaceholders,
                             schemaTree->containsEncryptedNode(),
                             parsedDistinct.serialize(cmdObj).body};
}

/**
 * Asserts that if _id is encrypted, it is provided explicitly. In addition, asserts
 * that no top level field in 'doc' has the value Timestamp(0,0). In both of those cases mongod
 * would usually generate a value so they cannot be encrypted here.
 */
void verifyNoGeneratedEncryptedFields(BSONObj doc, const EncryptionSchemaTreeNode& schemaTree) {
    uassert(51130,
            "_id must be explicitly provided when configured as encrypted",
            !schemaTree.getEncryptionMetadataForPath(FieldRef("_id")) || doc["_id"]);
    // Top level Timestamp(0, 0) is not allowed to be inserted because the server replaces it
    // with a different generated value. A nested Timestamp(0,0) does not have this issue.
    for (auto&& element : doc) {
        if (schemaTree.getEncryptionMetadataForPath(FieldRef(element.fieldNameStringData()))) {
            uassert(51129,
                    "A command that inserts cannot supply Timestamp(0, 0) for an encrypted "
                    "top-level field at path " +
                        element.fieldNameStringData(),
                    element.type() != BSONType::bsonTimestamp ||
                        element.timestamp() != Timestamp(0, 0));
        }
    }
}

PlaceHolderResult addPlaceHoldersForFindAndModify(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const std::string& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto request(uassertStatusOK(FindAndModifyRequest::parseFromBSON(
        CommandHelpers::parseNsCollectionRequired(dbName, cmdObj), cmdObj)));

    auto newQuery = replaceEncryptedFieldsInFilter(expCtx, *schemaTree.get(), request.getQuery());

    bool anythingEncrypted = false;
    if (auto updateMod = request.getUpdate()) {
        uassert(ErrorCodes::NotImplemented,
                "Pipeline updates not yet supported on mongocryptd",
                updateMod->type() == write_ops::UpdateModification::Type::kClassic);

        if (request.isUpsert()) {
            verifyNoGeneratedEncryptedFields(updateMod->getUpdateClassic(), *schemaTree.get());
        }

        auto newUpdate = replaceEncryptedFieldsInUpdate(
            expCtx, *schemaTree.get(), updateMod->getUpdateClassic(), request.getArrayFilters());
        if (newUpdate.hasEncryptionPlaceholders) {
            request.setUpdateObj(newUpdate.result);
            anythingEncrypted = true;
        }
    }

    if (newQuery.hasEncryptionPlaceholders) {
        request.setQuery(newQuery.result);
        anythingEncrypted = true;
    }

    return PlaceHolderResult{anythingEncrypted,
                             schemaTree->containsEncryptedNode(),
                             anythingEncrypted ? request.toBSON(cmdObj) : cmdObj};
}

PlaceHolderResult addPlaceHoldersForInsert(OperationContext* opCtx,
                                           const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto batch = InsertOp::parse(request);
    auto docs = batch.getDocuments();
    PlaceHolderResult retPlaceholder;
    std::vector<BSONObj> docVector;
    for (const BSONObj& doc : docs) {
        verifyNoGeneratedEncryptedFields(doc, *schemaTree.get());
        // The insert command cannot currently perform any collation-aware comparisons, and
        // therefore does not accept a collation argument.
        const CollatorInterface* collator = nullptr;
        auto placeholderPair = replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, FieldRef(), doc, collator);
        retPlaceholder.hasEncryptionPlaceholders =
            retPlaceholder.hasEncryptionPlaceholders || placeholderPair.hasEncryptionPlaceholders;
        docVector.push_back(placeholderPair.result);
    }
    batch.setDocuments(docVector);
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("documents"_sd);
    retPlaceholder.result = removeExtraFields(fieldNames, batch.toBSON(request.body));
    retPlaceholder.schemaRequiresEncryption = schemaTree->containsEncryptedNode();
    return retPlaceholder;
}

PlaceHolderResult addPlaceHoldersForUpdate(OperationContext* opCtx,
                                           const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto updateOp = UpdateOp::parse(request);
    auto updates = updateOp.getUpdates();
    std::vector<write_ops::UpdateOpEntry> updateVector;
    PlaceHolderResult phr;

    for (auto&& update : updateOp.getUpdates()) {
        auto& updateMod = update.getU();
        auto collator = parseCollator(opCtx, update.getCollation());
        boost::intrusive_ptr<ExpressionContext> expCtx(
            new ExpressionContext(opCtx, collator.get()));

        uassert(ErrorCodes::NotImplemented,
                "Pipeline updates not yet supported on mongocryptd",
                updateMod.type() == write_ops::UpdateModification::Type::kClassic);
        if (update.getUpsert()) {
            verifyNoGeneratedEncryptedFields(updateMod.getUpdateClassic(), *schemaTree.get());
        }
        auto newFilter = replaceEncryptedFieldsInFilter(expCtx, *schemaTree.get(), update.getQ());
        auto newUpdate = replaceEncryptedFieldsInUpdate(expCtx,
                                                        *schemaTree.get(),
                                                        updateMod.getUpdateClassic(),
                                                        write_ops::arrayFiltersOf(update));
        // Create a non-const copy.
        auto newEntry = update;
        newEntry.setQ(newFilter.result);
        newEntry.setU(newUpdate.result);
        updateVector.push_back(newEntry);
        phr.hasEncryptionPlaceholders = phr.hasEncryptionPlaceholders ||
            newUpdate.hasEncryptionPlaceholders || newFilter.hasEncryptionPlaceholders;
    }

    updateOp.setUpdates(updateVector);
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("updates"_sd);
    phr.result = removeExtraFields(fieldNames, updateOp.toBSON(request.body));
    phr.schemaRequiresEncryption = schemaTree->containsEncryptedNode();
    return phr;
}

PlaceHolderResult addPlaceHoldersForDelete(OperationContext* opCtx,
                                           const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    invariant(schemaTree);
    PlaceHolderResult placeHolderResult{};

    auto deleteRequest = write_ops::Delete::parse(IDLParserErrorContext("delete"), request);
    std::vector<write_ops::DeleteOpEntry> markedDeletes;
    for (auto&& op : deleteRequest.getDeletes()) {
        markedDeletes.push_back(op);
        auto& opToMark = markedDeletes.back();
        auto collator = parseCollator(opCtx, op.getCollation());
        boost::intrusive_ptr<ExpressionContext> expCtx(
            new ExpressionContext(opCtx, collator.get()));

        auto resultForOp = replaceEncryptedFieldsInFilter(expCtx, *schemaTree, opToMark.getQ());
        placeHolderResult.hasEncryptionPlaceholders =
            placeHolderResult.hasEncryptionPlaceholders || resultForOp.hasEncryptionPlaceholders;
        opToMark.setQ(resultForOp.result);
    }

    deleteRequest.setDeletes(std::move(markedDeletes));
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("deletes"_sd);
    placeHolderResult.result = removeExtraFields(fieldNames, deleteRequest.toBSON(request.body));
    placeHolderResult.schemaRequiresEncryption = schemaTree->containsEncryptedNode();
    return placeHolderResult;
}

void serializePlaceholderResult(const PlaceHolderResult& placeholder, BSONObjBuilder* builder) {
    builder->append(kHasEncryptionPlaceholders, placeholder.hasEncryptionPlaceholders);
    builder->append(kSchemaRequiresEncryption, placeholder.schemaRequiresEncryption);
    builder->append(kResult, placeholder.result);
}

OpMsgRequest makeHybrid(const OpMsgRequest& request, BSONObj body) {
    OpMsgRequest newRequest;
    newRequest.body = body;
    newRequest.sequences = request.sequences;
    return newRequest;
}

using WriteOpProcessFunction =
    PlaceHolderResult(OperationContext* opCtx,
                      const OpMsgRequest& request,
                      std::unique_ptr<EncryptionSchemaTreeNode> schemaTree);

void processWriteOpCommand(OperationContext* opCtx,
                           const OpMsgRequest& request,
                           BSONObjBuilder* builder,
                           WriteOpProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(request.body, &stripped);

    auto newRequest = makeHybrid(request, stripped.obj());

    // Parse the JSON Schema to an encryption schema tree.
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    PlaceHolderResult placeholder = func(opCtx, newRequest, std::move(schemaTree));

    serializePlaceholderResult(placeholder, builder);
}

using QueryProcessFunction =
    PlaceHolderResult(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                      const std::string& dbName,
                      const BSONObj& cmdObj,
                      std::unique_ptr<EncryptionSchemaTreeNode> schemaTree);

void processQueryCommand(OperationContext* opCtx,
                         const std::string& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         QueryProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(cmdObj, &stripped);

    // Parse the JSON Schema to an encryption schema tree.
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    auto collator = extractCollator(opCtx, cmdObj);
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(opCtx, collator.get()));

    PlaceHolderResult placeholder = func(expCtx, dbName, stripped.obj(), std::move(schemaTree));
    auto fieldNames = cmdObj.getFieldNames<std::set<StringData>>();

    // A new camel-case name of the FindAndModify command needs to be used
    // in place of the legacy one.
    if (fieldNames.count(FindAndModifyRequest::kLegacyCommandName)) {
        fieldNames.insert(FindAndModifyRequest::kCommandName);
    }
    placeholder.result = removeExtraFields(fieldNames, placeholder.result);

    serializePlaceholderResult(placeholder, builder);
}

}  // namespace

PlaceHolderResult replaceEncryptedFields(BSONObj doc,
                                         const EncryptionSchemaTreeNode* schema,
                                         EncryptionPlaceholderContext placeholderContext,
                                         FieldRef leadingPath,
                                         const boost::optional<BSONObj>& origDoc,
                                         const CollatorInterface* collator) {
    PlaceHolderResult res;
    res.result = replaceEncryptedFieldsRecursive(schema,
                                                 doc,
                                                 placeholderContext,
                                                 origDoc,
                                                 collator,
                                                 &leadingPath,
                                                 &res.hasEncryptionPlaceholders);
    return res;
}

void processFindCommand(OperationContext* opCtx,
                        const std::string& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* builder) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForFind);
}

void processAggregateCommand(OperationContext* opCtx,
                             const std::string& dbName,
                             const BSONObj& cmdObj,
                             BSONObjBuilder* builder) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForAggregate);
}

void processDistinctCommand(OperationContext* opCtx,
                            const std::string& dbName,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* builder) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForDistinct);
}

void processCountCommand(OperationContext* opCtx,
                         const std::string& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForCount);
}

void processFindAndModifyCommand(OperationContext* opCtx,
                                 const std::string& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForFindAndModify);
}

void processInsertCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForInsert);
}

void processUpdateCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForUpdate);
}

void processDeleteCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForDelete);
}

BSONObj buildEncryptPlaceholder(BSONElement elem,
                                const ResolvedEncryptionInfo& metadata,
                                EncryptionPlaceholderContext placeholderContext,
                                const CollatorInterface* collator,
                                const boost::optional<BSONObj>& origDoc,
                                const boost::optional<const EncryptionSchemaTreeNode&> schema) {
    // There are more stringent requirements for which encryption placeholders are legal in the
    // context of a query which makes a comparison to an encrypted field. For instance, comparisons
    // are only possible against deterministically encrypted fields, whereas either the random or
    // deterministic encryption algorithms are legal in the context of a placeholder for a write.
    if (placeholderContext == EncryptionPlaceholderContext::kComparison) {
        uassert(51158,
                "Cannot query on fields encrypted with the randomized encryption algorithm",
                metadata.algorithm == FleAlgorithmEnum::kDeterministic);

        switch (elem.type()) {
            case BSONType::String:
            case BSONType::Symbol: {
                uassert(31054,
                        str::stream()
                            << "cannot apply non-simple collation when comparing to element "
                            << elem
                            << " with client-side encryption",
                        !collator);
                break;
            }
            default:
                break;
        }
    }

    uassert(31009,
            str::stream() << "Cannot encrypt a field containing an array: " << elem
                          << " with the deterministic algorithm.",
            elem.type() != BSONType::Array || metadata.algorithm == FleAlgorithmEnum::kRandom);

    if (metadata.bsonTypeSet) {
        uassert(31118,
                str::stream() << "Cannot encrypt element of type " << typeName(elem.type())
                              << " because schema requires that type is one of: "
                              << typeSetToString(*metadata.bsonTypeSet),
                metadata.bsonTypeSet->hasType(elem.type()));
    }

    FleAlgorithmInt integerAlgorithm = metadata.algorithm == FleAlgorithmEnum::kDeterministic
        ? FleAlgorithmInt::kDeterministic
        : FleAlgorithmInt::kRandom;

    EncryptionPlaceholder marking(integerAlgorithm, EncryptSchemaAnyType(elem));

    const auto& keyId = metadata.keyId;
    if (keyId.type() == EncryptSchemaKeyId::Type::kUUIDs) {
        marking.setKeyId(keyId.uuids()[0]);
    } else {
        uassert(51093, "A non-static (JSONPointer) keyId is not supported.", origDoc);
        auto pointer = keyId.jsonPointer();
        auto resolvedKey = pointer.evaluate(origDoc.get());
        uassert(51114,
                "keyId pointer '" + pointer.toString() + "' must point to a field that exists",
                resolvedKey);
        uassert(30017,
                "keyId pointer '" + pointer.toString() + "' cannot point to an encrypted field",
                !(schema->getEncryptionMetadataForPath(pointer.toFieldRef())));
        uassert(51115,
                "keyId pointer '" + pointer.toString() + "' must point to a string",
                resolvedKey.type() == BSONType::String);
        marking.setKeyAltName(resolvedKey.valueStringData());
    }

    // Serialize the placeholder to BSON.
    BSONObjBuilder bob;
    marking.serialize(&bob);
    auto markingObj = bob.done();

    // Encode the placeholder BSON as BinData (sub-type 6 for encryption). Prepend the sub-subtype
    // byte represent the intent-to-encrypt marking before the BSON payload.
    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(FleBlobSubtype::IntentToEncrypt);
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        elem.fieldNameStringData(), binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

Value buildEncryptPlaceholder(Value input,
                              const ResolvedEncryptionInfo& metadata,
                              EncryptionPlaceholderContext placeholderContext,
                              const CollatorInterface* collator) {
    StringData wrappingKey;
    // We cannot convert a Value directly into a BSONElement. So we wrap the 'input' into a Document
    // with key as 'wrappingKey' and then unwrap before returning.
    return Value(buildEncryptPlaceholder(Document{{wrappingKey, input}}.toBson().firstElement(),
                                         metadata,
                                         placeholderContext,
                                         collator,
                                         boost::none,
                                         boost::none)[wrappingKey]);
}

}  // namespace mongo