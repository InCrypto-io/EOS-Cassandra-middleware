package cassandra_storage

import (
	"strings"
)


type DataFilter struct {
	keywords []string
}


//s - keywords separated with space
func NewDataFilter(s string) *DataFilter {
	keywords := strings.Split(s, " ")
	return &DataFilter{ keywords: keywords }
}


func (f *DataFilter) IsOk(trace ActionTraceDoc) bool {
	data := trace.Act["data"]
	if m, ok := data.(map[string]interface{}); ok {
		for key, val := range m {
			for _, word := range f.keywords {
				if strings.Contains(key, word) {
					return true
				}
				if s, ok := val.(string); ok {
					if strings.Contains(s, word) {
						return true
					}
				}
			}
		}
	}
	return false
}