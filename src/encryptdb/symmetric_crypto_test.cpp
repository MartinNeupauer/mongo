/**
 *    Copyright (C) 2015 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "symmetric_crypto.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

// make space for a padding block
const int maxPTSize = (4 + 1) * crypto::aesBlockSize;

// AES CBC mode test vectors from NIST sp800-38a
const struct {
    size_t keySize;
    int mode;
    size_t ptLen;
    uint8_t key[maxPTSize];
    uint8_t iv[maxPTSize];
    uint8_t pt[maxPTSize];
    uint8_t ct[maxPTSize];
} aesTests[] = {
    // AES-128 CBC
    {crypto::sym128KeySize,
     crypto::cbcMode,
     4 * crypto::aesBlockSize,
     {0x2b,
      0x7e,
      0x15,
      0x16,
      0x28,
      0xae,
      0xd2,
      0xa6,
      0xab,
      0xf7,
      0x15,
      0x88,
      0x09,
      0xcf,
      0x4f,
      0x3c},
     {0x00,
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08,
      0x09,
      0x0a,
      0x0b,
      0x0c,
      0x0d,
      0x0e,
      0x0f},
     {0x6b,
      0xc1,
      0xbe,
      0xe2,
      0x2e,
      0x40,
      0x9f,
      0x96,
      0xe9,
      0x3d,
      0x7e,
      0x11,
      0x73,
      0x93,
      0x17,
      0x2a,
      0xae,
      0x2d,
      0x8a,
      0x57,
      0x1e,
      0x03,
      0xac,
      0x9c,
      0x9e,
      0xb7,
      0x6f,
      0xac,
      0x45,
      0xaf,
      0x8e,
      0x51,
      0x30,
      0xc8,
      0x1c,
      0x46,
      0xa3,
      0x5c,
      0xe4,
      0x11,
      0xe5,
      0xfb,
      0xc1,
      0x19,
      0x1a,
      0x0a,
      0x52,
      0xef,
      0xf6,
      0x9f,
      0x24,
      0x45,
      0xdf,
      0x4f,
      0x9b,
      0x17,
      0xad,
      0x2b,
      0x41,
      0x7b,
      0xe6,
      0x6c,
      0x37,
      0x10},
     {0x76,
      0x49,
      0xab,
      0xac,
      0x81,
      0x19,
      0xb2,
      0x46,
      0xce,
      0xe9,
      0x8e,
      0x9b,
      0x12,
      0xe9,
      0x19,
      0x7d,
      0x50,
      0x86,
      0xcb,
      0x9b,
      0x50,
      0x72,
      0x19,
      0xee,
      0x95,
      0xdb,
      0x11,
      0x3a,
      0x91,
      0x76,
      0x78,
      0xb2,
      0x73,
      0xbe,
      0xd6,
      0xb8,
      0xe3,
      0xc1,
      0x74,
      0x3b,
      0x71,
      0x16,
      0xe6,
      0x9e,
      0x22,
      0x22,
      0x95,
      0x16,
      0x3f,
      0xf1,
      0xca,
      0xa1,
      0x68,
      0x1f,
      0xac,
      0x09,
      0x12,
      0x0e,
      0xca,
      0x30,
      0x75,
      0x86,
      0xe1,
      0xa7}},
    // AES-256 CBC
    {crypto::sym256KeySize,
     crypto::cbcMode,
     4 * crypto::aesBlockSize,
     {0x60,
      0x3d,
      0xeb,
      0x10,
      0x15,
      0xca,
      0x71,
      0xbe,
      0x2b,
      0x73,
      0xae,
      0xf0,
      0x85,
      0x7d,
      0x77,
      0x81,
      0x1f,
      0x35,
      0x2c,
      0x07,
      0x3b,
      0x61,
      0x08,
      0xd7,
      0x2d,
      0x98,
      0x10,
      0xa3,
      0x09,
      0x14,
      0xdf,
      0xf4},
     {0x00,
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08,
      0x09,
      0x0a,
      0x0b,
      0x0c,
      0x0d,
      0x0e,
      0x0f},
     {0x6b,
      0xc1,
      0xbe,
      0xe2,
      0x2e,
      0x40,
      0x9f,
      0x96,
      0xe9,
      0x3d,
      0x7e,
      0x11,
      0x73,
      0x93,
      0x17,
      0x2a,
      0xae,
      0x2d,
      0x8a,
      0x57,
      0x1e,
      0x03,
      0xac,
      0x9c,
      0x9e,
      0xb7,
      0x6f,
      0xac,
      0x45,
      0xaf,
      0x8e,
      0x51,
      0x30,
      0xc8,
      0x1c,
      0x46,
      0xa3,
      0x5c,
      0xe4,
      0x11,
      0xe5,
      0xfb,
      0xc1,
      0x19,
      0x1a,
      0x0a,
      0x52,
      0xef,
      0xf6,
      0x9f,
      0x24,
      0x45,
      0xdf,
      0x4f,
      0x9b,
      0x17,
      0xad,
      0x2b,
      0x41,
      0x7b,
      0xe6,
      0x6c,
      0x37,
      0x10},
     {0xf5,
      0x8c,
      0x4c,
      0x04,
      0xd6,
      0xe5,
      0xf1,
      0xba,
      0x77,
      0x9e,
      0xab,
      0xfb,
      0x5f,
      0x7b,
      0xfb,
      0xd6,
      0x9c,
      0xfc,
      0x4e,
      0x96,
      0x7e,
      0xdb,
      0x80,
      0x8d,
      0x67,
      0x9f,
      0x77,
      0x7b,
      0xc6,
      0x70,
      0x2c,
      0x7d,
      0x39,
      0xf2,
      0x33,
      0x69,
      0xa9,
      0xd9,
      0xba,
      0xcf,
      0xa5,
      0x30,
      0xe2,
      0x63,
      0x04,
      0x23,
      0x14,
      0x61,
      0xb2,
      0xeb,
      0x05,
      0xe2,
      0xc3,
      0x9b,
      0xe9,
      0xfc,
      0xda,
      0x6c,
      0x19,
      0x07,
      0x8c,
      0x6a,
      0x9d,
      0x1b}}};

TEST(CryptoVectors, AES) {
    uint8_t pt[maxPTSize];
    uint8_t ct[maxPTSize];

    size_t numTests = sizeof(aesTests) / sizeof(aesTests[0]);
    for (size_t i = 0; i < numTests; i++) {
        size_t ctLen = 0;
        Status ret = crypto::aesEncrypt(aesTests[i].pt,
                                        aesTests[i].ptLen,
                                        aesTests[i].key,
                                        aesTests[i].keySize,
                                        aesTests[i].mode,
                                        aesTests[i].iv,
                                        ct,
                                        &ctLen);

        ASSERT(ret.isOK()) << "aesEncrypt failed " << ret;

        // Only compare the ptLen first bytes and ignore padding
        ASSERT(ctLen == (aesTests[i].ptLen + crypto::aesBlockSize) &&
               0 == memcmp(aesTests[i].ct, ct, aesTests[i].ptLen))
            << "AES encrypt mismatch in iteration " << i;

        size_t ptLen = 0;
        ret = crypto::aesDecrypt(ct,
                                 ctLen,
                                 aesTests[i].key,
                                 aesTests[i].keySize,
                                 aesTests[i].mode,
                                 aesTests[i].iv,
                                 pt,
                                 &ptLen);

        ASSERT(ret.isOK()) << "aesDecrypt failed " << ret;

        ASSERT(ptLen == aesTests[i].ptLen && 0 == memcmp(aesTests[i].pt, pt, aesTests[i].ptLen))
            << "AES decrypt mismatch in iteration " << i;
    }
}

}  // namespace
}  // namespace mongo
