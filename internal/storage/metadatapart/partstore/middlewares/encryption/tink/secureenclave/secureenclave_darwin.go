//go:build darwin

package secureenclave

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Security -framework Foundation -framework CoreFoundation

#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>

// Helper to create CFString from C string
static CFStringRef createCFString(const char *str) {
    return CFStringCreateWithCString(kCFAllocatorDefault, str, kCFStringEncodingUTF8);
}

// getOrCreateSecureEnclaveKeyC gets or creates a P-256 key in the Secure Enclave.
// Returns the SecKeyRef or NULL on error. Error message returned via errOut.
static SecKeyRef getOrCreateSecureEnclaveKeyC(const char *keyLabel, char **errOut) {
    CFStringRef label = createCFString(keyLabel);
    if (label == NULL) {
        *errOut = strdup("failed to create label string");
        return NULL;
    }

    // First, try to find existing key
    CFMutableDictionaryRef query = CFDictionaryCreateMutable(
        kCFAllocatorDefault, 0,
        &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);

    CFDictionarySetValue(query, kSecClass, kSecClassKey);
    CFDictionarySetValue(query, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    CFDictionarySetValue(query, kSecAttrLabel, label);
    CFDictionarySetValue(query, kSecAttrTokenID, kSecAttrTokenIDSecureEnclave);
    CFDictionarySetValue(query, kSecReturnRef, kCFBooleanTrue);

    SecKeyRef existingKey = NULL;
    OSStatus status = SecItemCopyMatching(query, (CFTypeRef *)&existingKey);
    CFRelease(query);

    if (status == errSecSuccess && existingKey != NULL) {
        CFRelease(label);
        return existingKey;
    }

    // Key doesn't exist, create new one
    CFMutableDictionaryRef accessControlAttrs = CFDictionaryCreateMutable(
        kCFAllocatorDefault, 0,
        &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);

    // Private key attributes - no biometrics, accessible after first unlock (even when locked)
    SecAccessControlRef accessControl = SecAccessControlCreateWithFlags(
        kCFAllocatorDefault,
        kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,  // Accessible when locked, after first unlock since boot
        0,  // No additional flags (no biometrics)
        NULL);

    if (accessControl == NULL) {
        CFRelease(label);
        CFRelease(accessControlAttrs);
        *errOut = strdup("failed to create access control");
        return NULL;
    }

    CFDictionarySetValue(accessControlAttrs, kSecAttrAccessControl, accessControl);
    CFDictionarySetValue(accessControlAttrs, kSecAttrIsPermanent, kCFBooleanTrue);
    CFDictionarySetValue(accessControlAttrs, kSecAttrLabel, label);

    // Key generation attributes
    CFMutableDictionaryRef keyAttrs = CFDictionaryCreateMutable(
        kCFAllocatorDefault, 0,
        &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);

    CFDictionarySetValue(keyAttrs, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    int keySize = 256;
    CFNumberRef keySizeNum = CFNumberCreate(kCFAllocatorDefault, kCFNumberIntType, &keySize);
    CFDictionarySetValue(keyAttrs, kSecAttrKeySizeInBits, keySizeNum);
    CFDictionarySetValue(keyAttrs, kSecAttrTokenID, kSecAttrTokenIDSecureEnclave);
    CFDictionarySetValue(keyAttrs, kSecPrivateKeyAttrs, accessControlAttrs);

    CFErrorRef error = NULL;
    SecKeyRef privateKey = SecKeyCreateRandomKey(keyAttrs, &error);

    CFRelease(keySizeNum);
    CFRelease(accessControlAttrs);
    CFRelease(keyAttrs);
    CFRelease(accessControl);
    CFRelease(label);

    if (privateKey == NULL) {
        if (error != NULL) {
            CFStringRef errorDesc = CFErrorCopyDescription(error);
            CFIndex length = CFStringGetLength(errorDesc);
            CFIndex maxSize = CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) + 1;
            char *buffer = (char *)malloc(maxSize);
            CFStringGetCString(errorDesc, buffer, maxSize, kCFStringEncodingUTF8);
            *errOut = buffer;
            CFRelease(errorDesc);
            CFRelease(error);
        } else {
            *errOut = strdup("unknown error creating Secure Enclave key");
        }
        return NULL;
    }

    return privateKey;
}

// getPublicKeyDataC extracts the public key data from a SecKeyRef.
// Returns the raw public key bytes or NULL on error.
static CFDataRef getPublicKeyDataC(SecKeyRef privateKey, char **errOut) {
    SecKeyRef publicKey = SecKeyCopyPublicKey(privateKey);
    if (publicKey == NULL) {
        *errOut = strdup("failed to get public key from private key");
        return NULL;
    }

    CFErrorRef error = NULL;
    CFDataRef publicKeyData = SecKeyCopyExternalRepresentation(publicKey, &error);
    CFRelease(publicKey);

    if (publicKeyData == NULL) {
        if (error != NULL) {
            CFStringRef errorDesc = CFErrorCopyDescription(error);
            CFIndex length = CFStringGetLength(errorDesc);
            CFIndex maxSize = CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) + 1;
            char *buffer = (char *)malloc(maxSize);
            CFStringGetCString(errorDesc, buffer, maxSize, kCFStringEncodingUTF8);
            *errOut = buffer;
            CFRelease(errorDesc);
            CFRelease(error);
        } else {
            *errOut = strdup("unknown error extracting public key");
        }
        return NULL;
    }

    return publicKeyData;
}

// secureEnclaveKeyExchangeC performs ECDH key exchange using the Secure Enclave private key.
// ephemeralPubKeyData: raw bytes of the ephemeral public key (uncompressed P-256 format)
// Returns the shared secret or NULL on error.
static CFDataRef secureEnclaveKeyExchangeC(SecKeyRef privateKey, const void *ephemeralPubKeyData, size_t ephemeralPubKeyLen, char **errOut) {
    // Create CFData from ephemeral public key bytes
    CFDataRef pubKeyData = CFDataCreate(kCFAllocatorDefault, (const UInt8 *)ephemeralPubKeyData, ephemeralPubKeyLen);
    if (pubKeyData == NULL) {
        *errOut = strdup("failed to create public key data");
        return NULL;
    }

    // Create attributes for the ephemeral public key
    CFMutableDictionaryRef keyAttrs = CFDictionaryCreateMutable(
        kCFAllocatorDefault, 0,
        &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);

    CFDictionarySetValue(keyAttrs, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    CFDictionarySetValue(keyAttrs, kSecAttrKeyClass, kSecAttrKeyClassPublic);
    int keySize = 256;
    CFNumberRef keySizeNum = CFNumberCreate(kCFAllocatorDefault, kCFNumberIntType, &keySize);
    CFDictionarySetValue(keyAttrs, kSecAttrKeySizeInBits, keySizeNum);

    // Create SecKey from the ephemeral public key data
    CFErrorRef error = NULL;
    SecKeyRef ephemeralPubKey = SecKeyCreateWithData(pubKeyData, keyAttrs, &error);
    CFRelease(pubKeyData);
    CFRelease(keySizeNum);
    CFRelease(keyAttrs);

    if (ephemeralPubKey == NULL) {
        if (error != NULL) {
            CFStringRef errorDesc = CFErrorCopyDescription(error);
            CFIndex length = CFStringGetLength(errorDesc);
            CFIndex maxSize = CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) + 1;
            char *buffer = (char *)malloc(maxSize);
            CFStringGetCString(errorDesc, buffer, maxSize, kCFStringEncodingUTF8);
            *errOut = buffer;
            CFRelease(errorDesc);
            CFRelease(error);
        } else {
            *errOut = strdup("failed to create ephemeral public key");
        }
        return NULL;
    }

    // Perform ECDH key exchange
    CFMutableDictionaryRef exchangeParams = CFDictionaryCreateMutable(
        kCFAllocatorDefault, 0,
        &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);

    error = NULL;
    CFDataRef sharedSecret = SecKeyCopyKeyExchangeResult(
        privateKey,
        kSecKeyAlgorithmECDHKeyExchangeStandard,
        ephemeralPubKey,
        exchangeParams,
        &error);

    CFRelease(ephemeralPubKey);
    CFRelease(exchangeParams);

    if (sharedSecret == NULL) {
        if (error != NULL) {
            CFStringRef errorDesc = CFErrorCopyDescription(error);
            CFIndex length = CFStringGetLength(errorDesc);
            CFIndex maxSize = CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) + 1;
            char *buffer = (char *)malloc(maxSize);
            CFStringGetCString(errorDesc, buffer, maxSize, kCFStringEncodingUTF8);
            *errOut = buffer;
            CFRelease(errorDesc);
            CFRelease(error);
        } else {
            *errOut = strdup("failed to perform ECDH key exchange");
        }
        return NULL;
    }

    return sharedSecret;
}

// releaseKey releases a SecKeyRef
static void releaseKey(SecKeyRef key) {
    if (key != NULL) {
        CFRelease(key);
    }
}

// releaseData releases a CFDataRef
static void releaseData(CFDataRef data) {
    if (data != NULL) {
        CFRelease(data);
    }
}
*/
import "C"

import (
	"crypto/ecdh"
	"fmt"
	"unsafe"
)

// secKeyRef is a reference to a SecKey in the Secure Enclave
type secKeyRef C.SecKeyRef

// getOrCreateSecureEnclaveKey gets or creates a P-256 key in the Secure Enclave.
func getOrCreateSecureEnclaveKey(keyLabel string) (secKeyRef, error) {
	cKeyLabel := C.CString(keyLabel)
	defer C.free(unsafe.Pointer(cKeyLabel))

	var errOut *C.char
	keyRef := C.getOrCreateSecureEnclaveKeyC(cKeyLabel, &errOut)

	if keyRef == 0 {
		if errOut != nil {
			defer C.free(unsafe.Pointer(errOut))
			return 0, fmt.Errorf("%s", C.GoString(errOut))
		}
		return 0, fmt.Errorf("failed to get or create Secure Enclave key")
	}

	return secKeyRef(keyRef), nil
}

// getPublicKey extracts the public key from the Secure Enclave key reference.
func getPublicKey(keyRef secKeyRef) (*ecdh.PublicKey, error) {
	var errOut *C.char
	pubKeyData := C.getPublicKeyDataC(C.SecKeyRef(keyRef), &errOut)

	if pubKeyData == 0 {
		if errOut != nil {
			defer C.free(unsafe.Pointer(errOut))
			return nil, fmt.Errorf("%s", C.GoString(errOut))
		}
		return nil, fmt.Errorf("failed to get public key")
	}
	defer C.releaseData(pubKeyData)

	// Convert CFData to Go bytes
	length := C.CFDataGetLength(pubKeyData)
	ptr := C.CFDataGetBytePtr(pubKeyData)
	pubKeyBytes := C.GoBytes(unsafe.Pointer(ptr), C.int(length))

	// Parse as ECDH public key (the data is in uncompressed SEC1 format)
	publicKey, err := ecdh.P256().NewPublicKey(pubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	return publicKey, nil
}

// secureEnclaveECDH performs ECDH key exchange using the Secure Enclave private key.
func secureEnclaveECDH(keyRef secKeyRef, ephemeralPublic *ecdh.PublicKey) ([]byte, error) {
	ephemeralPubBytes := ephemeralPublic.Bytes()

	var errOut *C.char
	sharedSecretData := C.secureEnclaveKeyExchangeC(
		C.SecKeyRef(keyRef),
		unsafe.Pointer(&ephemeralPubBytes[0]),
		C.size_t(len(ephemeralPubBytes)),
		&errOut)

	if sharedSecretData == 0 {
		if errOut != nil {
			defer C.free(unsafe.Pointer(errOut))
			return nil, fmt.Errorf("%s", C.GoString(errOut))
		}
		return nil, fmt.Errorf("failed to perform ECDH")
	}
	defer C.releaseData(sharedSecretData)

	// Convert CFData to Go bytes
	length := C.CFDataGetLength(sharedSecretData)
	ptr := C.CFDataGetBytePtr(sharedSecretData)
	sharedSecret := C.GoBytes(unsafe.Pointer(ptr), C.int(length))

	return sharedSecret, nil
}

// releaseKeyRef releases the Secure Enclave key reference.
func releaseKeyRef(keyRef secKeyRef) error {
	C.releaseKey(C.SecKeyRef(keyRef))
	return nil
}
