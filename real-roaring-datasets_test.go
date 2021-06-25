package roaring

import "testing"

func TestRetrieval(t *testing.T) {
	arrays, e := RetrieveRealDataBitmaps("uscensus2000")
	if e != nil {
		t.Errorf("error: cannot read the file")
	}
	if len(arrays) != 200 {
		t.Errorf("bad count")
	}
}
