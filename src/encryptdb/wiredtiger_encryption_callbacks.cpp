/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include <iostream>
#include <string>

#include "encryption_key_manager.h"
#include "encryption_options.h"
#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/random.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "symmetric_crypto.h"

namespace mongo {

namespace {
std::unique_ptr<PseudoRandom> prng;
MONGO_INITIALIZER(SeedPRNG)(InitializerContext* context) {
    std::unique_ptr<SecureRandom> sr(SecureRandom::create());
    prng.reset(new PseudoRandom(sr->nextInt64()));
    return Status::OK();
}

/**
 * The WiredTiger encryption API consists of the following callbacks:
 *
 * sizing    - Return the maximum padding an encryption operation might
 *             add to the plaintext data.
 *
 * customize - Customize a WT_ENCRYPTOR struct for a specific keyid with
 *             the correct key.
 *
 * encrypt   - Encrypt data.
 *
 * decrypt   - Decrypt data.
 *
 * terminate - Called when a WT_ENCRYPTOR is no longer used and its
 *             resources can be freed.
 *
 * The ExtendedWTEncryptor struct is used by the customize function
 * to store data that WiredTiger does not need to know about, but will
 * store for us. WiredTiger will cast the ExtendedWTEncryptor* to a
 * WT_ENCRYPTOR* and ignore any other members.
 *
 * The stored ExtendedWTEncryptor* will be passed back in the encrypt,
 * decrypt and terminate callbacks.
 */
struct ExtendedWTEncryptor {
    WT_ENCRYPTOR encryptor;      // Must come first
    SymmetricKey* symmetricKey;  // Symmetric key
    crypto::aesMode cipherMode;  // Cipher mode
};

int sizing(WT_ENCRYPTOR* encryptor, WT_SESSION* session, size_t* expansionConstant) {
    try {
        // Add a constant factor for WiredTiger to know how much extra memory it must allocate,
        // at most, for encryption.
        // The modes have different amounts of overhead. GCM has 12(tag) + 12(IV) bytes, and CBC
        // has 16(IV) + 16(max padding overhead). Use CBC's which is larger.
        *expansionConstant = 32;
        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::sizing: " << exceptionToStatus();
        fassertFailed(4037);
    }
}

int customize(WT_ENCRYPTOR* encryptor,
              WT_SESSION* session,
              WT_CONFIG_ARG* encryptConfig,
              WT_ENCRYPTOR** customEncryptor) {
    try {
        WT_EXTENSION_API* extApi = session->connection->get_extension_api(session->connection);

        const ExtendedWTEncryptor* origEncryptor =
            reinterpret_cast<const ExtendedWTEncryptor*>(encryptor);
        std::unique_ptr<ExtendedWTEncryptor> myEncryptor(new ExtendedWTEncryptor);

        // Get the key id from the encryption configuration
        WT_CONFIG_ITEM keyIdItem;
        if (0 != extApi->config_get(extApi, session, encryptConfig, "keyid", &keyIdItem) ||
            keyIdItem.len == 0) {
            error() << "Unable to retrieve keyid when customizing encryptor";
            return EINVAL;
        }
        std::string keyId = std::string(keyIdItem.str, keyIdItem.len);

        // Get the name from the encryption configuration
        WT_CONFIG_ITEM encryptorNameItem;
        if (0 != extApi->config_get(extApi, session, encryptConfig, "name", &encryptorNameItem) ||
            encryptorNameItem.len == 0) {
            error() << "Unable to retrieve name when customizing encryptor";
            return EINVAL;
        }
        std::string encryptorName = std::string(encryptorNameItem.str, encryptorNameItem.len);
        if (mongo::encryptionGlobalParams.encryptionCipherMode != encryptorName) {
            severe() << "Invalid cipher mode '"
                     << mongo::encryptionGlobalParams.encryptionCipherMode << "', expected '"
                     << encryptorName << "'";
            return EINVAL;
        }


        // Get the symmetric key for the key id
        StatusWith<std::unique_ptr<SymmetricKey>> swSymmetricKey =
            EncryptionKeyManager::get(getGlobalServiceContext())->getKey(keyId);
        if (!swSymmetricKey.isOK()) {
            error() << "Unable to initialize encryption. " << swSymmetricKey.getStatus().reason();
            return EINVAL;
        }
        *(myEncryptor.get()) = *origEncryptor;

        myEncryptor.get()->symmetricKey = swSymmetricKey.getValue().release();
        myEncryptor.get()->cipherMode = crypto::getCipherModeFromString(encryptorName);

        *customEncryptor = &(myEncryptor.release()->encryptor);
        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::customize: " << exceptionToStatus();
        fassertFailed(4045);
    }
}

int encrypt(WT_ENCRYPTOR* encryptor,
            WT_SESSION* session,
            uint8_t* src,
            size_t srcLen,
            uint8_t* dst,
            size_t dstLen,
            size_t* resultLen) {
    try {
        const ExtendedWTEncryptor* crypto = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);
        if (!src || !dst || !resultLen || !crypto || !crypto->symmetricKey) {
            return EINVAL;
        }

        crypto::EncryptedMemoryLayout layout(crypto->cipherMode, dst, dstLen);

        if (!layout.canFitPlaintext(srcLen)) {
            severe() << "Insufficient memory allocated for encryption.";
            return ENOMEM;
        }

        // Generate IV, this is currently very predictable
        // FIXME: Make this unpredictable to prevent chosen plaintext attacks
        int64_t iv[2] = {prng->nextInt64(), prng->nextInt64()};
        memcpy(layout.getIV(), iv, layout.getIVSize());

        Status ret =
            crypto::aesEncrypt(src, srcLen, &layout, *crypto->symmetricKey, crypto->cipherMode);

        if (!ret.isOK()) {
            severe() << "Encrypt error for key " << crypto->symmetricKey->getKeyId() << ": " << ret;
            return EINVAL;
        }

        // Check the returned length, including block size padding
        if (layout.getDataSize() != layout.expectedCiphertextLen(srcLen)) {
            log() << "Encrypt error, expected cipher text of length "
                  << layout.expectedCiphertextLen(srcLen) << "] but found " << *resultLen;
            return EINVAL;
        }

        *resultLen = layout.getHeaderSize() + layout.getDataSize();

        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::encrypt: " << exceptionToStatus();
        fassertFailed(4046);
    }
}

int decrypt(WT_ENCRYPTOR* encryptor,
            WT_SESSION* session,
            uint8_t* src,
            size_t srcLen,
            uint8_t* dst,
            size_t dstLen,
            size_t* resultLen) {
    try {
        const ExtendedWTEncryptor* crypto = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);
        if (!src || !dst || !resultLen || !crypto || !crypto->symmetricKey) {
            return EINVAL;
        }

        crypto::EncryptedMemoryLayout layout(crypto->cipherMode, src, srcLen);

        Status ret =
            crypto::aesDecrypt(&layout, *crypto->symmetricKey, crypto->cipherMode, dst, resultLen);

        if (!ret.isOK()) {
            if (crypto->symmetricKey->getKeyId() == kSystemKeyId) {
                severe()
                    << "Decryption failed, invalid encryption master key or keystore encountered.";
            } else {
                severe() << "Decrypt error for key " << crypto->symmetricKey->getKeyId() << ": "
                         << ret;
            }
            return EINVAL;
        }
        // Check the returned length, excluding headers block padding
        size_t lowerBound, upperBound;
        std::tie(lowerBound, upperBound) = layout.expectedPlaintextLen();
        if (*resultLen < lowerBound || *resultLen > upperBound) {
            log() << "Decrypt error, expected clear text length in interval"
                  << "[" << lowerBound << "," << upperBound << "]"
                  << "but found " << *resultLen;
            return EINVAL;
        }

        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::decrypt: " << exceptionToStatus();
        fassertFailed(4047);
    }
}

int destroyEncryptor(WT_ENCRYPTOR* encryptor, WT_SESSION* session) {
    try {
        ExtendedWTEncryptor* myEncryptor = reinterpret_cast<ExtendedWTEncryptor*>(encryptor);

        if (myEncryptor) {
            // Destroy the SymmetricKey object
            if (myEncryptor->symmetricKey) {
                delete myEncryptor->symmetricKey;
            }
            delete myEncryptor;
        }
        return 0;
    } catch (...) {
        // Prevent C++ exceptions from propagating into C code
        severe() << "Aborting due to exception in WT_ENCRYPTOR::terminate: " << exceptionToStatus();
        fassertFailed(4048);
    }
}

}  // namespace
}  // namespace mongo

/**
 * mongo_addWiredTigerEncryptors is the entry point for the WiredTiger crypto
 * callback API. WiredTiger uses dlsym to identify the entry point based on
 * configuration parameters to the wiredtiger_open call.
 *
 * This function adds a single AES encryptor. The API is flexible enough to
 * support multiple different encryptors but at the moment we are using a single one.
 */
extern "C" MONGO_COMPILER_API_EXPORT int mongo_addWiredTigerEncryptors(WT_CONNECTION* connection) {
    mongo::ExtendedWTEncryptor* extWTEncryptorCBC = new mongo::ExtendedWTEncryptor;
    extWTEncryptorCBC->symmetricKey = nullptr;
    extWTEncryptorCBC->encryptor.sizing = mongo::sizing;
    extWTEncryptorCBC->encryptor.customize = mongo::customize;
    extWTEncryptorCBC->encryptor.encrypt = mongo::encrypt;
    extWTEncryptorCBC->encryptor.decrypt = mongo::decrypt;
    extWTEncryptorCBC->encryptor.terminate = mongo::destroyEncryptor;

    int ret;
    if (mongo::encryptionGlobalParams.enableEncryption &&
        (ret = connection->add_encryptor(connection,
                                         mongo::encryptionGlobalParams.encryptionCipherMode.c_str(),
                                         reinterpret_cast<WT_ENCRYPTOR*>(extWTEncryptorCBC),
                                         nullptr)) != 0) {
        return ret;
    }
    return 0;
}
