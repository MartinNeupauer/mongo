/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include <cstdint>

#include "mongo/base/status.h"

namespace mongo {
namespace crypto {

/**
 * Encryption algorithm identifiers and block sizes
 */
const uint8_t aesAlgorithm = 0x1;

/**
 * Block and key sizes
 */
const size_t aesBlockSize = 16;
const size_t sym128KeySize = 16;
const size_t sym256KeySize = 32;

/**
 * Min and max symmetric key lengths
 */
const size_t minKeySize = 16;
const size_t maxKeySize = 32;

/**
 * Encryption mode identifiers
 */
const uint8_t cbcMode = 0x10;
const uint8_t gcmMode = 0x20;

/**
 * Encrypts the plaintext 'in' using AES with 'key' and block size 'keySize'
 * using encryption mode 'mode'. Supported modes are CBC and GCM.
 *
 * 'outLen' is an in-out parameter representing the size of the buffer 'out', and the
 * resulting length of the encrypted buffer.
 */
Status aesEncrypt(const uint8_t* in,
                  size_t inLen,
                  const uint8_t* key,
                  size_t keySize,
                  int mode,
                  const uint8_t* iv,
                  uint8_t* out,
                  size_t* outLen);

/**
 * Decrypts the plaintext 'in' using AES with 'key' and block size 'keySize'
 * using encryption mode 'mode'. Supported modes are CBC and GCM.
 *
 * 'outLen' is an in-out parameter representing the size of the buffer 'out', and the
 * resulting length of the decrypted buffer.
 */
Status aesDecrypt(const uint8_t* in,
                  size_t inLen,
                  const uint8_t* key,
                  size_t keySize,
                  int mode,
                  const uint8_t* iv,
                  uint8_t* out,
                  size_t* outLen);

}  // namespace crypto
}  // namespace mongo
