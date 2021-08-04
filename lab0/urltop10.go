package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs = append(kvs, KeyValue{Key: l})
	}
	return URLCountCombine(kvs)
}

// URLCountCombine is the combine function in the first round
func URLCountCombine(kvs []KeyValue) []KeyValue {
	clusteredKvs := make(map[string][]string)
	for _, kv := range kvs {
		clusteredKvs[kv.Key] = append(clusteredKvs[kv.Key], kv.Value)
	}
	combinedKvs := make([]KeyValue, 0)
	for k, v := range clusteredKvs {
		//merge the record counts with same url
		combinedKvs = append(combinedKvs, KeyValue{Key: k, Value: strconv.Itoa(len(v))})
	}
	return combinedKvs
}

// URLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	//sum up the counts from each map result
	sum := 0
	for _, v := range values {
		cnt, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		sum += cnt
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
}

// URLTop10Map is the map function in the second round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	values := make([]string, 0)
	for _, l := range lines {
		values = append(values, l)
	}
	//get top10 in each map phase
	combinedKvs := URLTop10Combine("", values)
	return combinedKvs
}

//URLTop10Combine is the combine function in the second round
func URLTop10Combine(key string, values []string) []KeyValue {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}
	kvs := make([]KeyValue, 0)
	us, cs := TopN(cnts, 10)

	// construct the kv structure for reduce phase
	for i := range us {
		kvs = append(kvs, KeyValue{"", us[i]+" "+strconv.Itoa(cs[i])})
	}
	return kvs
}


// URLTop10Reduce is the reduce function in the second round
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}