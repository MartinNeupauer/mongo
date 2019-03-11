/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

static const uint8_t uuidBytes[] = {0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
static const BSONObj encryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId"
                           << BSON_ARRAY(BSONBinData(uuidBytes, 16, newUUID))));
static const BSONObj pointerEncryptObj =
    BSON("encrypt" << BSON("algorithm"
                           << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                           << "keyId"
                           << "/key"));

/**
 * Builds a schema with a single encrypted field using the passed in 'encrypt'
 * specification. The 'encrypt' data is a single-element object of the format
 * {encrypt: {...}}.
 */
BSONObj buildBasicSchema(BSONObj encryptData) {
    return BSON("properties" << BSON("foo" << encryptData) << "type"
                             << "object");
}

void assertEncryptedCorrectly(PlaceHolderResult response,
                              BSONElement elem,
                              BSONObj metadataobj,
                              BSONElement orig) {
    ASSERT_TRUE(response.hasEncryptionPlaceholders);
    int len;
    auto rawBinData = elem.binData(len);
    ASSERT_GT(len, 0);
    ASSERT_EQ(rawBinData[0], 0);
    ASSERT_TRUE(elem.isBinData(BinDataType::Encrypt));
    IDLParserErrorContext ctx("queryAnalysis");
    auto correctPlaceholder =
        buildEncryptPlaceholder(orig, EncryptionMetadata::parse(ctx, metadataobj["encrypt"].Obj()));
    ASSERT_BSONELT_EQ(correctPlaceholder[elem.fieldNameStringData()], elem);
}

BSONObj encodePlaceholder(std::string fieldName, EncryptionPlaceholder toSerialize) {
    BSONObjBuilder bob;
    toSerialize.serialize(&bob);
    auto markingObj = bob.done();

    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(0);
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        fieldName, binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

TEST(IsEncryptionNeededTests, IsEncryptedNotPresent) {
    auto input = BSON("properties" << BSON("foo" << BSONObj()));

    ASSERT_FALSE(isEncryptionNeeded(input));
}

TEST(IsEncryptionNeededTests, isEncryptionNeededDeepEncrypt) {
    auto input = BSON("properties" << BSON("a" << BSON("type"
                                                       << "object"
                                                       << "properties"
                                                       << BSON("b" << encryptObj))
                                               << "c"
                                               << BSONObj())
                                   << "type"
                                   << "object");

    ASSERT_TRUE(isEncryptionNeeded(input));
}

TEST(ReplaceEncryptedFieldsTest, ReplacesTopLevelFieldCorrectly) {
    auto schema = buildBasicSchema(encryptObj);
    auto doc = BSON("foo"
                    << "toEncrypt");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    BSONElement encryptedElem = replaceRes.result["foo"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["foo"]);
}

TEST(ReplaceEncryptedFieldsTest, ReplacesSecondLevelFieldCorrectly) {
    auto schema = BSON("properties" << BSON("a" << BSON("type"
                                                        << "object"
                                                        << "properties"
                                                        << BSON("b" << encryptObj))
                                                << "c"
                                                << BSONObj())
                                    << "type"
                                    << "object");
    auto doc = BSON("a" << BSON("b"
                                << "foo")
                        << "c"
                        << "bar");
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    BSONElement encryptedElem = replaceRes.result["a"]["b"];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["a"]["b"]);
    BSONElement notEncryptedElem = replaceRes.result["c"];
    ASSERT_FALSE(notEncryptedElem.type() == BSONType::BinData);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentTreatedAsFieldName) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << encryptObj))));
    auto doc = BSON("foo" << BSON(0 << "encrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    BSONElement encryptedElem = replaceRes.result["foo"][0];
    assertEncryptedCorrectly(replaceRes, encryptedElem, encryptObj, doc["foo"][0]);
}

TEST(ReplaceEncryptedFieldsTest, NumericPathComponentNotTreatedAsArrayIndex) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON(0 << encryptObj))));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto doc = BSON("foo" << BSON_ARRAY("notEncrypted"));
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    ASSERT_FALSE(replaceRes.hasEncryptionPlaceholders);
    doc = BSON("foo" << BSON_ARRAY(BSON(0 << "notEncrypted") << BSON(0 << "alsoNotEncrypted")));
    replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    ASSERT_FALSE(replaceRes.hasEncryptionPlaceholders);
}

TEST(ReplaceEncryptedFieldsTest, ObjectInArrayWithSameNameNotEncrypted) {
    auto schema = BSON("type"
                       << "object"
                       << "properties"
                       << BSON("foo" << BSON("type"
                                             << "object"
                                             << "properties"
                                             << BSON("bar" << encryptObj))));
    auto doc = BSON("foo" << BSON_ARRAY("bar"
                                        << "notEncrypted"));
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);
    auto replaceRes = replaceEncryptedFields(doc, schemaTree.get());
    ASSERT_BSONOBJ_EQ(doc, replaceRes.result);
}

TEST(BuildEncryptPlaceholderTest, JSONPointerResolvesCorrectly) {
    auto schema = buildBasicSchema(pointerEncryptObj);

    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << "value");
    EncryptionPlaceholder expected(FleAlgorithmEnum::kRandom, EncryptSchemaAnyType(doc["foo"]));
    auto keyAltName = BSON("key"
                           << "value");
    expected.setKeyAltName(EncryptSchemaAnyType(keyAltName["key"]));
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    auto response = buildEncryptPlaceholder(doc["foo"], metadata, doc);
    auto correctBSON = encodePlaceholder("foo", expected);
    ASSERT_BSONOBJ_EQ(correctBSON, response);
}

TEST(BuildEncryptPlaceholderTest, JSONPointerResolvesCorrectlyThroughArray) {
    auto localEncryptObj = BSON("encrypt" << BSON("algorithm"
                                                  << "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                                  << "keyId"
                                                  << "/key/0"));
    auto schema = buildBasicSchema(localEncryptObj);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << BSON_ARRAY("value"));
    EncryptionPlaceholder expected(FleAlgorithmEnum::kRandom, EncryptSchemaAnyType(doc["foo"]));
    auto keyAltName = BSON("key"
                           << "value");
    expected.setKeyAltName(EncryptSchemaAnyType(keyAltName["key"]));
    EncryptionMetadata metadata =
        EncryptionMetadata::parse(IDLParserErrorContext("meta"), localEncryptObj["encrypt"].Obj());
    auto response = buildEncryptPlaceholder(doc["foo"], metadata, doc);
    auto correctBSON = encodePlaceholder("foo", expected);
    ASSERT_BSONOBJ_EQ(correctBSON, response);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToObject) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << BSON("Forbidden"
                            << "key"));
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    ASSERT_THROWS_CODE(
        buildEncryptPlaceholder(doc["foo"], metadata, doc), AssertionException, 51115);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToArray) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    BSONObjBuilder builder;
    builder.append("foo", "encrypt");
    builder.appendCodeWScope("key",
                             "This is javascript code;",
                             BSON("Scope"
                                  << "Here"));
    auto doc = builder.obj();
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    ASSERT_THROWS_CODE(
        buildEncryptPlaceholder(doc["foo"], metadata, doc), AssertionException, 51115);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerPointsToCode) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto doc = BSON("foo"
                    << "encrypt"
                    << "key"
                    << BSON_ARRAY("Forbidden"
                                  << "key"));
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    ASSERT_THROWS_CODE(
        buildEncryptPlaceholder(doc["foo"], metadata, doc), AssertionException, 51115);
}

TEST(BuildEncryptPlaceholderTest, UAssertIfPointerDoesNotEvaluate) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto doc = BSON("foo"
                    << "encrypt");
    EncryptionPlaceholder expected(FleAlgorithmEnum::kRandom, EncryptSchemaAnyType(doc["foo"]));
    auto keyAltName = BSON("key"
                           << "value");
    expected.setKeyAltName(EncryptSchemaAnyType(keyAltName["key"]));
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    ASSERT_THROWS_CODE(
        buildEncryptPlaceholder(doc["foo"], metadata, doc), AssertionException, 51114);
}

TEST(BuildEncryptPlaceholderTest, PointedToUUIDActsAsKeyIdInsteadOfAltName) {
    auto schema = buildBasicSchema(pointerEncryptObj);
    auto uuid = UUID::gen();
    BSONObjBuilder bob;
    bob.append("foo", "encrypt");
    uuid.appendToBuilder(&bob, "key");
    auto doc = bob.obj();

    EncryptionPlaceholder expected(FleAlgorithmEnum::kRandom, EncryptSchemaAnyType(doc["foo"]));
    expected.setKeyId(uuid);
    EncryptionMetadata metadata = EncryptionMetadata::parse(IDLParserErrorContext("meta"),
                                                            pointerEncryptObj["encrypt"].Obj());
    auto response = buildEncryptPlaceholder(doc["foo"], metadata, doc);
    auto correctBSON = encodePlaceholder("foo", expected);
    ASSERT_BSONOBJ_EQ(correctBSON, response);
}

}  // namespace
}  // namespace mongo
