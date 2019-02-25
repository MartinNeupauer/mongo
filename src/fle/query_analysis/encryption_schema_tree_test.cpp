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

#include "mongo/platform/basic.h"

#include "encryption_schema_tree.h"

#include "mongo/bson/json.h"
#include "mongo/db/bson/bson_helper.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {

/**
 * Parses 'schema' into an encryption schema tree and returns the EncryptionMetadata for
 * 'path'.
 */
EncryptionMetadata extractMetadata(BSONObj schema, std::string path) {
    auto result = EncryptionSchemaTreeNode::parse(schema);
    auto metadata = result->getEncryptionMetadataForPath(FieldRef(path));
    ASSERT(metadata);
    return metadata.get();
}

/**
 * Parses 'schema' into an encryption schema tree and verifies that 'path' is not encrypted.
 */
void assertNotEncrypted(BSONObj schema, std::string path) {
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(result->getEncryptionMetadataForPath(FieldRef(path)));
}

TEST(EncryptionSchemaTreeTest, MarksTopLevelFieldAsEncrypted) {
    const auto uuid = UUID::gen();
    EncryptionMetadata metadata;
    metadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    metadata.setKeyId(EncryptSchemaKeyId({uuid}));

    BSONObj schema =
        fromjson(R"({
            type: "object",
            properties: {
                ssn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                    }
                },
                name: {
                    type: "string"
                }
            }
        })");
    ASSERT(extractMetadata(schema, "ssn") == metadata);
    assertNotEncrypted(schema, "ssn.nonexistent");
    assertNotEncrypted(schema, "name");
}

TEST(EncryptionSchemaTreeTest, MarksNestedFieldsAsEncrypted) {
    const auto uuid = UUID::gen();
    EncryptionMetadata metadata;
    metadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    metadata.setKeyId(EncryptSchemaKeyId({uuid}));

    BSONObj schema =
        fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: "object",
                properties: {
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                        }
                    }
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(extractMetadata(schema, "user.ssn") == metadata);
    assertNotEncrypted(schema, "user");
    assertNotEncrypted(schema, "user.name");
}

TEST(EncryptionSchemaTreeTest, MarksNumericFieldNameAsEncrypted) {
    const auto uuid = UUID::gen();
    EncryptionMetadata metadata;
    metadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    metadata.setKeyId(EncryptSchemaKeyId({uuid}));
    BSONObj encryptObj = fromjson(R"({
        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
        keyId: [)" + uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) +
                                  R"(]})");

    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("0" << BSON("encrypt" << encryptObj)));
    ASSERT(extractMetadata(schema, "0") == metadata);

    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("nested" << BSON("type"
                                           << "object"
                                           << "properties"
                                           << BSON("0" << BSON("encrypt" << encryptObj)))));
    ASSERT(extractMetadata(schema, "nested.0") == metadata);
}

TEST(EncryptionSchemaTreeTest, MarksMultipleFieldsAsEncrypted) {
    const auto uuid = UUID::gen();
    const auto uuidStr = uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false);
    EncryptionMetadata metadata;
    metadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    metadata.setKeyId(EncryptSchemaKeyId({uuid}));

    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {            
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [)" +
                              uuidStr + R"(]
                }
            },
            accountNumber: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [)" +
                              uuidStr + R"(]
                }
            },
            super: {
                type: "object",
                properties: {
                    secret: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [)" +
                              uuidStr + R"(]
                        }
                    }
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "ssn") == metadata);
    ASSERT(extractMetadata(schema, "accountNumber") == metadata);
    ASSERT(extractMetadata(schema, "super.secret") == metadata);
    assertNotEncrypted(schema, "super");
}

TEST(EncryptionSchemaTreeTest, TopLevelEncryptMarksEmptyPathAsEncrypted) {
    const auto uuid = UUID::gen();
    const auto uuidStr = uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false);
    EncryptionMetadata metadata;
    metadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    metadata.setKeyId(EncryptSchemaKeyId({uuid}));

    BSONObj schema = fromjson(R"({
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [)" +
                              uuidStr + R"(]
                }})");

    ASSERT(extractMetadata(schema, "") == metadata);
}

TEST(EncryptionSchemaTreeTest, ExtractsCorrectMetadataOptions) {
    const auto uuid = UUID::gen();
    EncryptionMetadata ssnMetadata;
    ssnMetadata.setAlgorithm(FleAlgorithmEnum::kDeterministic);
    ssnMetadata.setInitializationVector(ConstDataRange(NULL, static_cast<size_t>(0)));
    ssnMetadata.setKeyId(EncryptSchemaKeyId({uuid}));

    const IDLParserErrorContext encryptCtxt("encrypt");
    auto metadataObj = BSON("algorithm"
                            << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                            << "initializationVector"
                            << BSONBinData(NULL, 0, BinDataType::BinDataGeneral)
                            << "keyId"
                            << BSON_ARRAY(uuid));

    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("ssn" << BSON("encrypt" << metadataObj)));
    ASSERT(extractMetadata(schema, "ssn") == ssnMetadata);
    ASSERT(extractMetadata(schema, "ssn") == EncryptionMetadata::parse(encryptCtxt, metadataObj));
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptAlongsideAnotherTypeKeyword) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
                type: "object"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
                bsonType: "BinData"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptWithSiblingKeywords) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
                properties: {invalid: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                },
                minimum: 5,
                items: {}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfConflictingEncryptKeywords) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {},
                properties: {
                    invalid: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                },
                items: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                type: "array", 
                items: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object",
        properties: {
            user: {
                type: "object",
                properties: {
                    ssn: {
                        type: "array", 
                        items: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                            }
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinAdditionalItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                type: "array",
                items: {},
                additionalItems: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                additionalItems: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsNotTypeRestricted) {
    BSONObj schema = fromjson(R"({
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object",
        properties: {
            user: {
                properties: {
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsTypeRestrictedWithMultipleTypes) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            user: {
                type: ["object", "array"],
                properties: {
                    ssn: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidSchema) {
    BSONObj schema = fromjson(R"({properties: "invalid"})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);

    schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: "invalid"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseUnknownFieldInEncrypt) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {unknownField: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 40415);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidAlgorithm) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-SomeInvalidAlgo",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::BadValue);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidIV) {
    BSONObj schema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("ssn" << BSON("encrypt" << BSON("initializationVector" << BSONBinData(
                                                         nullptr, 0, BinDataType::MD5Type)))));
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidKeyIdUUID) {
    BSONObj schema =
        BSON("type"
             << "object"
             << "properties"
             << BSON("ssn" << BSON("encrypt" << BSON("keyId" << BSON_ARRAY(BSONBinData(
                                                         nullptr, 0, BinDataType::MD5Type))))));
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51084);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptBelowAdditionalPropertiesWithoutTypeObject) {
    BSONObj schema = fromjson(R"({
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfIllegalSubschemaUnderAdditionalProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            },
            illegal: 1
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfAdditionalPropertiesIsWrongType) {
    BSONObj schema = fromjson("{additionalProperties: [{type: 'string'}]}");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfAdditionalPropertiesWithEncryptInsideItems) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            arr: {
                type: "array",
                items: {
                    type: "object",
                    additionalProperties: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesAllPropertiesCorrectlyReportedAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
            }
        }
    })");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz"}));
}

TEST(EncryptionSchemaTreeTest,
     NestedAdditionalPropertiesAllPropertiesCorrectlyReportedAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            obj: {
                type: "object",
                additionalProperties: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.baz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other.foo"}));
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesOnlyAppliesToFieldsNotNamedByProperties) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            obj: {
                type: "object",
                properties: {
                    a: {type: "string"},
                    b: {type: "object"},
                    c: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                        }
                    }
                },
                additionalProperties: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.foo"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.baz"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.b"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.b.c"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.c"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"other.foo"}));
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesWorksWithNestedPropertiesSubschema) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            type: "object",
            properties: {
                a: {type: "string"},
                b: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                    }
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.b"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar.b"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar.a"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar.c"}));
}

TEST(EncryptionSchemaTreeTest, AdditionalPropertiesWorksWithNestedAdditionalPropertiesSubschema) {
    BSONObj schema = fromjson(R"({
        type: "object",
        additionalProperties: {
            type: "object",
            additionalProperties: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [{$binary: "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"}]
                }
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"bar"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"baz"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.bar"}));
    ASSERT(encryptionTree->getEncryptionMetadataForPath(FieldRef{"foo.baz"}));
}

TEST(EncryptionSchemaTreeTest, CanSuccessfullyParseAdditionalItemsWhenBoolean) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            arr: {
                type: "array",
                additionalItems: true
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"arr"}));
}

TEST(EncryptionSchemaTreeTest, CanSuccessfullyParseAdditionalPropertiesWhenBoolean) {
    BSONObj schema = fromjson(R"({
        type: "object",
        properties: {
            obj: {
                type: "object",
                additionalProperties: false
            }
        }})");
    auto encryptionTree = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.foo"}));
    ASSERT_FALSE(encryptionTree->getEncryptionMetadataForPath(FieldRef{"obj.bar"}));
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataWithOverriding) {
    const auto uuid = UUID::gen();
    const char initVector[] = "mongo";

    EncryptionMetadata secretMetadata;
    secretMetadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    secretMetadata.setKeyId(EncryptSchemaKeyId({uuid}));

    EncryptionMetadata ssnMetadata;
    ssnMetadata.setAlgorithm(FleAlgorithmEnum::kDeterministic);
    ssnMetadata.setInitializationVector(
        ConstDataRange(initVector, initVector + sizeof(initVector) - 1));
    ssnMetadata.setKeyId(EncryptSchemaKeyId({uuid}));

    BSONObj schema =
        fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
            keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
        },
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    initializationVector: {$binary: "bW9uZ28=", $type: "00"}
                }
            },
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "ssn") == ssnMetadata);
    ASSERT(extractMetadata(schema, "super.secret") == secretMetadata);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMultipleLevels) {
    const char initVector[] = "mongo";
    const auto uuid1 = UUID::gen();
    const auto uuid2 = UUID::gen();

    EncryptionMetadata secretMetadata;
    secretMetadata.setAlgorithm(FleAlgorithmEnum::kDeterministic);
    secretMetadata.setInitializationVector(
        ConstDataRange(initVector, initVector + sizeof(initVector) - 1));
    secretMetadata.setKeyId(EncryptSchemaKeyId({uuid1}));

    EncryptionMetadata mysteryMetadata;
    mysteryMetadata.setAlgorithm(FleAlgorithmEnum::kRandom);
    mysteryMetadata.setKeyId(EncryptSchemaKeyId({uuid2}));

    BSONObj schema =
        fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
        },
        type: "object",
        properties: {
            super: {
                encryptMetadata: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    initializationVector: {$binary: "bW9uZ28=", $type: "00"},
                    keyId: [)" +
                 uuid1.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                },
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            },
            duper: {
                type: "object",
                properties: {
                    mystery: {
                        encrypt: {
                            keyId: [)" +
                 uuid2.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
                        }
                    }
                }
            }
        }})");
    ASSERT(extractMetadata(schema, "super.secret") == secretMetadata);
    ASSERT(extractMetadata(schema, "duper.mystery") == mysteryMetadata);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMissingAlgorithm) {
    const auto uuid = UUID::gen();

    BSONObj schema =
        fromjson(R"({
        encryptMetadata: {
            keyId: [)" +
                 uuid.toBSON().getField("uuid").jsonString(JsonStringFormat::Strict, false) + R"(]
        },
        type: "object",
        properties: {
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51095);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMissingInitializationVector) {
    BSONObj schema = fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
        },
        type: "object",
        properties: {
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51096);
}

TEST(EncryptionSchemaTreeTest, InheritEncryptMetadataMissingKeyId) {
    BSONObj schema = fromjson(R"({
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            initializationVector: {$binary: "bW9uZ28=", $type: "00"}
        },
        type: "object",
        properties: {
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51097);
}

}  // namespace
}  // namespace mongo
