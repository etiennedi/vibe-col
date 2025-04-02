package col

// encodeIDs encodes the IDs based on the encoding type
func (bw *BufferedWriter) encodeIDs(ids []uint64) ([]uint64, [][]byte, uint32, error) {
	return encodeData(bw.encodingType, ids, deltaEncode, encodeVarInt)
}

// encodeValues encodes the values based on the encoding type
func (bw *BufferedWriter) encodeValues(values []int64) ([]int64, [][]byte, uint32, error) {
	return encodeData(bw.encodingType, values, deltaEncodeInt64, encodeSignedVarInt)
}
