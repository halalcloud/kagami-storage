package mimetype

import (
	"encoding/hex"
	"strings"
)

var suffixMimeMap = map[string]string{
	"txt":        "text/plain",
	"jpg":        "image/jpeg",
	"jpeg":       "image/jpeg",
	"gif":        "image/gif",
	"png":        "image/png",
	"tiff":       "image/tiff",
	"bmp":        "image/bmp",
	"dwg":        "application/acad",
	"html":       "text/html",
	"htm":        "text/html",
	"css":        "text/css",
	"js":         "application/x-javascript",
	"rtf":        "application/rtf",
	"psd":        "image/vnd.adobe.photoshop",
	"eml":        "message/rfc822",
	"doc":        "application/msword",
	"vsd":        "application/vsd",
	"mdb":        "application/x-msaccess",
	"ps":         "application/postscript",
	"pdf":        "application/pdf",
	"rmvb":       "application/vnd.rn-realmedia-vbr",
	"flv":        "video/x-flv",
	"mp4":        "video/mp4v-es",
	"mp3":        "audio/x-mpeg3",
	"mpg":        "video/mpeg",
	"wmv":        "video/x-ms-wmv",
	"wav":        "audio/wav",
	"avi":        "video/avi",
	"mid":        "audio/mid",
	"zip":        "application/zip",
	"rar":        "application/rar",
	"ini":        "text/plain",
	"jar":        "application/x-java-applet",
	"exe":        "application/dos-exe",
	"jsp":        "text/jsp",
	"mf":         "text/mf",
	"xml":        "text/xml",
	"sql":        "text/sql",
	"java":       "text/x-java-source",
	"bat":        "application/bat",
	"gz":         "application/gzip",
	"properties": "text/properties",
	"class":      "application/x-java-class",
	"chm":        "application/octet-stream",
	"docx":       "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"wps":        "application/vnd.ms-works",
	"torrent":    "application/x-bittorrent",
}

var fileHeaderMimeMap = map[string]string{
	"ffd8ffe000104a464946": "image/jpeg",
	"89504e470d0a1a0a0000": "image/gif",
	"47494638396126026f01": "image/png",
	"49492a00227105008037": "image/tiff",
	"424d228c010000000000": "image/bmp", //16bit
	"424d8240090000000000": "image/bmp", //24bit
	"424d8e1b030000000000": "image/bmp", //256bit
	"41433130313500000000": "application/acad",
	"3c21444f435459504520": "text/html",
	"48544d4c207b0d0a0942": "text/css",
	"696b2e71623d696b2e71": "application/x-javascript",
	"7b5c727466315c616e73": "application/rtf",
	"38425053000100000000": "image/vnd.adobe.photoshop",
	"46726f6d3a203d3f6762": "message/rfc822",
	"d0cf11e0a1b11ae10000": "application/msword",
	//"d0cf11e0a1b11ae10000": "application/vsd",
	"5374616E64617264204A": "application/x-msaccess",
	"252150532D41646F6265": "application/postscript",
	"255044462d312e350d0a": "application/pdf",
	"2e524d46000000120001": "application/vnd.rn-realmedia-vbr",
	"464c5601050000000900": "video/x-flv",
	"00000020667479706d70": "video/mp4v-es",
	"49443303000000002176": "audio/x-mpeg3",
	"000001ba210001000180": "video/mpeg",
	"3026b2758e66cf11a6d9": "video/x-ms-wmv",
	"52494646e27807005741": "audio/wav",
	"52494646d07d60074156": "video/avi",
	"4d546864000000060001": "audio/mid",
	"504b0304140000000800": "application/zip",
	"526172211a0700cf9073": "application/rar",
	"235468697320636f6e66": "text/plain",
	"504b03040a0000000000": "application/x-java-applet",
	"4d5a9000030000000400": "application/dos-exe",
	"3c25402070616765206c": "text/jsp",
	"4d616e69666573742d56": "text/mf",
	"3c3f786d6c2076657273": "text/xml",
	"494e5345525420494e54": "text/sql",
	"7061636b616765207765": "text/x-java-source",
	"406563686f206f66660d": "application/bat",
	"1f8b0800000000000000": "application/gzip",
	"6c6f67346a2e726f6f74": "text/properties",
	"cafebabe0000002e0041": "application/x-java-class",
	"49545346030000006000": "application/octet-stream",
	"504b0304140006000800": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	//"d0cf11e0a1b11ae10000": "application/vnd.ms-works",
	"6431303a637265617465": "application/x-bittorrent",
}

func SuffixMime(filename string) (mime string) {
	filename = strings.ToLower(filename)
	strs := strings.Split(filename, ".")
	return suffixMimeMap[strs[len(strs)-1]]
}

func FileHeaderMime(fileHeader []byte) (mime string) {
	return fileHeaderMimeMap[strings.ToLower(hex.EncodeToString(fileHeader))]
}

/*
	1.获取文件类型 优先 文件名 其次 文件内容前5字节，最后unknown
	2.首先根据form上传文件判断，其次根据key 判断
*/
func Check_uploadfile_type(filename, key string, fileHeader []byte, default_mime string) (mime string) {
	if filename != "" {
		mime = SuffixMime(filename)
	}
	if mime == "" && key != "" {
		mime = SuffixMime(key)
	}
	if mime == "" && len(fileHeader) != 0 {
		mime = FileHeaderMime(fileHeader)
	}
	if mime == "" {
		mime = default_mime
	}
	return
}

/*:
func main() {
	fmt.Println(SuffixMime("ok.png"))
	f, _ := os.Open("/data01/project/go/src/test/hadoop(ha)&hbase(双master)安装.doc")
	buf := make([]byte, 5)
	io.ReadFull(f, buf)
	fmt.Println(FileHeaderMime(buf))
}
*/
