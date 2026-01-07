package jobs

import (
	"io"

	"github.com/pyrohost/elytra/src/server"
)

// hashFileMurmur2 calculates the CurseForge-specific Murmur2 hash for a file.
// CurseForge uses a modified MurmurHash2 algorithm where whitespace bytes
// (tab, newline, carriage return, space) are filtered out before hashing.
func hashFileMurmur2(s *server.Server, filePath string) (uint32, error) {
	f, err := s.Filesystem().UnixFS().Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Read entire file into memory
	// For large files, this could be optimized to stream, but for mod files
	// (typically < 100MB) this is acceptable
	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}

	// Filter out whitespace bytes: 9 (tab), 10 (newline), 13 (carriage return), 32 (space)
	filtered := make([]byte, 0, len(data))
	for _, b := range data {
		if b != 9 && b != 10 && b != 13 && b != 32 {
			filtered = append(filtered, b)
		}
	}

	// Calculate MurmurHash2 with seed = 1
	return murmurHash2(filtered, 1), nil
}

// murmurHash2 implements the MurmurHash2 algorithm.
// This is the standard MurmurHash2 implementation used by CurseForge.
func murmurHash2(data []byte, seed uint32) uint32 {
	const (
		m = uint32(0x5bd1e995)
		r = 24
	)

	h := seed ^ uint32(len(data))
	i := 0

	// Process 4 bytes at a time
	for i+4 <= len(data) {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24

		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k

		i += 4
	}

	// Handle remaining bytes (1-3)
	switch len(data) - i {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}

	// Final mix
	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}

