package wal

import (
	"encoding/binary"
	"hash/crc32"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/wal/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func CrcSum(data []byte) uint32 {
	crc := crc32.New(crcTable)
	_, _ = crc.Write(data)
	return crc.Sum32()
}

func Uint32ToBytes(crc uint32) []byte {
	b := make([]byte, binary.Size(crc))
	binary.LittleEndian.PutUint32(b, crc)
	return b
}

// FromBytes parses record from []bytes
func FromBytes(data []byte) (*pb.Record, error) {
	r := new(pb.Record)
	err := proto.Unmarshal(data, r)
	return r, err
}

// ToBytes parses record from []bytes
func ToBytes(m pb.Record) ([]byte, error) {
	return proto.Marshal(&m)
}

func GetTime(m *pb.Record) (time.Time, error) {
	return ptypes.Timestamp(m.Timestamp)
}
