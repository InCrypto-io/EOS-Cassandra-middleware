package cassandra_storage

import (
	"fmt"
)


type Range interface {
	IsEmpty() bool
	Format(string) string
}


type TimestampRange struct {
	Start *Timestamp
	End   *Timestamp

	StartStrict bool
	EndStrict   bool
}


func NewTimestampRange(start *Timestamp, startStrict bool, end *Timestamp, endStrict bool) TimestampRange {
	return TimestampRange{ Start: start, End: end, StartStrict: startStrict, EndStrict: endStrict }
}


func (t TimestampRange) IsEmpty() bool {
	return t.Start == nil && t.End == nil
}

func (t TimestampRange) Format(fieldName string) string {
	startCompSign := ">"
	if !t.StartStrict {
		startCompSign += "="
	}
	endCompSign := "<"
	if !t.EndStrict {
		endCompSign += "="
	}
	s := ""
	if t.Start != nil {
		s += fmt.Sprintf(" %s %s '%s' ", fieldName, startCompSign, t.Start.String())
		if t.End != nil {
			s += fmt.Sprintf("AND %s %s '%s' ", fieldName, endCompSign, t.End.String())
		}
	} else if t.End != nil {
		s += fmt.Sprintf(" %s %s '%s' ", fieldName, endCompSign, t.End.String())
	}
	return s
}



type TimestampExact struct {
	T *Timestamp
}


func NewTimestampExact(t *Timestamp) TimestampExact {
	return TimestampExact{ T: t }
}


func (t TimestampExact) IsEmpty() bool {
	return t.T == nil
}

func (t TimestampExact) Format(fieldName string) string {
	if t.IsEmpty() {
		return ""
	}

	return fmt.Sprintf(" %s = '%s' ", fieldName, t.T.String())
}