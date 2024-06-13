package variable

type Variable struct {
	Bucket         string
	Key            string
	Etag           string
	Fname          string
	Fsize          string
	Mimetype       string
	EndUser        string
	PersistentId   string
	ImageInfo      *ImageInfoS
	AvInfo         *AvInfoS
	Ext            string
	Uuid           string
	BodySha1       string
	Customvariable map[string]string
}

type ImageInfoS struct {
	Size       int64  `json:"size"`
	Format     string `json:"format"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	ColorModel string `json:"colorModel"`
}

type AvInfoS struct {
	Audio  *AudioS
	Format *FormatS
	Video  *VideoS
}

type TagsS struct {
	CreationTime string
}

type AudioS struct {
	BitRate    string
	Channels   int
	CodecName  string
	CodecType  string
	Duration   string
	Index      int
	NbFrames   string
	RFrameRate string
	SampleFmt  string
	SampleRate string
	StartTime  string
	Tags       *TagsS
}

type FormatS struct {
	BitRate        string
	Duration       string
	FormatLongName string
	FormatName     string
	NbStreams      int
	Size           string
	StartTime      string
	Tags           *TagsS
}

type VideoS struct {
	BitRate            string
	CodecName          string
	CodecType          string
	DisplayAspectRatio string
	Duration           string
	Height             int
	Index              int
	NbFrames           string
	PixFmt             string
	RFrameRate         string
	SampleAspectRatio  string
	StartTime          string
	Width              int
	Tags               *TagsS
}

func (v *Variable) Getmagicvariable(key string) (value interface{}) {
	switch {
	case "$(bucket)" == key:
		value = v.Bucket
	case "$(key)" == key:
		value = v.Key
	case "$(etag)" == key:
		value = v.Etag
	case "$(fname)" == key:
		value = v.Fname
	case "$(fsize)" == key:
		value = v.Fsize
	case "$(mimeType)" == key:
		value = v.Mimetype
	case "$(endUser)" == key:
		value = v.EndUser
	case "$(ext)" == key:
		value = v.Ext
	case "$(uuid)" == key:
		value = v.Uuid
	case "$(bodySha1)" == key:
		value = v.BodySha1
	case "$(imageInfo)" == key:
		value = v.ImageInfo
	case "$(imageInfo.height)" == key:
		value = v.ImageInfo.Height
	case "$(imageInfo.width)" == key:
		value = v.ImageInfo.Width
	case "$(imageInfo.size)" == key:
		value = v.ImageInfo.Size
	case "$(imageInfo.format)" == key:
		value = v.ImageInfo.Format
	case "$(imageInfo.colorModel)" == key:
		value = v.ImageInfo.ColorModel
	//TODO persistentId,exif,avinfo,imageAve
	default:
		value = ""
	}
	return
}

func (v *Variable) Getcustomvariable(key string) (value string) {
	var (
		ok bool
	)
	if value, ok = v.Customvariable[key]; ok {
		return
	} else {
		value = ""
		return
	}

}
