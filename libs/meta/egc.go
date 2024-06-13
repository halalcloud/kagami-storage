package meta

type GCDestoryListRetOK struct {
	Marker     string         `json:"marker"`
	Trash_flag bool           `json:"trash_flag"`
	DList      []*DestroyFile `json:"dlist"` // type use DestroyFile for test
}
