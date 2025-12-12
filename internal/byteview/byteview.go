package byteview

type Byteview struct {
	b []byte
}

// New 创建一个 ByteView
func New(b []byte) Byteview {
	c := make([]byte, len(b))
	copy(c, b)
	return Byteview{b: c}
}

func (v Byteview) Len() int {
	return len(v.b)
}

func (v Byteview) ByteSlice() []byte {
	c := make([]byte, len(v.b))
	copy(c, v.b)
	return c
}

func (v Byteview) String() string {
	return string(v.b)
}
