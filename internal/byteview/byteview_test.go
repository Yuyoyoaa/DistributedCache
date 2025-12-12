package byteview

import "testing"

func TestByteViewCopy(t *testing.T) {
	data := []byte("hello")
	bv := New(data)

	data[0] = 'X'
	if string(bv.ByteSlice()) != "hello" {
		t.Fatalf("ByteView modified unexpectedly: %s", bv.ByteSlice())
	}
}

func TestByteViewLen(t *testing.T) {
	bv := New([]byte("12345"))
	if bv.Len() != 5 {
		t.Fatalf("unexpected Len(): %d", bv.Len())
	}
}
