package cassandra_storage

import (
	"fmt"
)


type TimestampRange struct {
	Start *Timestamp
	End   *Timestamp

	StartStrict bool
	EndStrict   bool
}


func NewTimestampRange(start *Timestamp, startStrict bool, end *Timestamp, endStrict bool) TimestampRange {
	return TimestampRange{ Start: start, End: end, StartStrict: startStrict, EndStrict: endStrict }
}


func (r *TimestampRange) IsEmpty() bool {
	return r.Start == nil && r.End == nil
}

func (r *TimestampRange) Format(fieldName string) string {
	startCompSign := ">"
	if !r.StartStrict {
		startCompSign += "="
	}
	endCompSign := "<"
	if !r.EndStrict {
		endCompSign += "="
	}
	s := ""
	if r.Start != nil {
		s += fmt.Sprintf(" %s %s '%s' ", fieldName, startCompSign, r.Start.String())
		if r.End != nil {
			s += fmt.Sprintf("AND %s %s '%s' ", fieldName, endCompSign, r.End.String())
		}
	} else if r.End != nil {
		s += fmt.Sprintf(" %s %s '%s' ", fieldName, endCompSign, r.End.String())
	}
	return s
}