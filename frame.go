package strata

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const maxFrame = 32 << 20

var checksum = crc32.MakeTable(crc32.Castagnoli)

// Independent header and payload checksums distinguish a broken size from a short tail.
func frame(payload []byte) ([]byte, error) {
	if len(payload) == 0 || len(payload) > maxFrame {
		return nil, fmt.Errorf("frame size %d outside supported range", len(payload))
	}
	b := make([]byte, 16+len(payload))
	copy(b, "ST01")
	binary.BigEndian.PutUint32(b[4:], uint32(len(payload)))
	binary.BigEndian.PutUint32(b[8:], crc32.Checksum(b[:8], checksum))
	binary.BigEndian.PutUint32(b[12:], crc32.Checksum(payload, checksum))
	copy(b[16:], payload)
	return b, nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var h [16]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(h[4:])
	if string(h[:4]) != "ST01" || crc32.Checksum(h[:8], checksum) != binary.BigEndian.Uint32(h[8:]) || size == 0 || size > maxFrame {
		return nil, errors.New("corrupt frame header")
	}
	p := make([]byte, size)
	if _, err := io.ReadFull(r, p); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if crc32.Checksum(p, checksum) != binary.BigEndian.Uint32(h[12:]) {
		return nil, errors.New("corrupt frame payload")
	}
	return p, nil
}
