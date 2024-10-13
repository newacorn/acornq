package acornq

import (
	"encoding/hex"
	"github.com/google/uuid"
)

func uuid2Bytes(uuid uuid.UUID) [36]byte {
	var buf [36]byte
	encodeHex(buf[:], uuid)
	return buf
}

func uuidBytes() [36]byte {
	var buf [36]byte
	encodeHex(buf[:], uuid.New())
	return buf
}

func encodeHex(dst []byte, uuid uuid.UUID) {
	hex.Encode(dst, uuid[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], uuid[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], uuid[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], uuid[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], uuid[10:])
}
