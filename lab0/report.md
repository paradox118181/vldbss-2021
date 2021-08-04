# Lab0 实验报告

## 实验结果

### 1. 完成 Map-Reduce 框架

`make test_example` 的实验截图

<img src="Screen Shot 2021-08-04 at 16.12.28.png" alt="Screen Shot 2021-08-04 at 16.12.28" style="zoom:50%;" align = "left"/>

### 2. 基于 Map-Reduce 框架编写 Map-Reduce 函数

`make test_homework` 的实验截图

<img src="Screen Shot 2021-08-04 at 23.54.43.png" alt="Screen Shot 2021-08-04 at 23.54.43" style="zoom:50%;" align="left"/>

## 实验总结

### MapReduce 框架

框架提供了 MapReduce Map 部分的实现，需要补充 Coordinator 和 Worker 里 Reduce 部分的代码。

主要遇到的问题是：

1. 一开始实现的时候没有注意到这个 MapReduce 框架是自动化多轮次的，每轮自动使用上一轮的结果，写好 MapReduce 框架以后才发现要在 round 之间传递中间结果。不过这个问题解决很简单，就是向 notify 传递一个中间结果文件名的列表即可，这个 notify channel 同时起到了向 test 函数通知任务完成的作用。

2. 困扰比较久的是执行时间问题，这个测试样例卡时间比较严，一开始用 8 核的 M1 MacBook Pro 测试会恰好在 1GB 数据量的 case9 超时，后面换了一台 8 核的 Intel 服务器同样超时，确定是需要进行一下优化。分析出主要耗时的部分是文件读写的 I/O 开销和 reduce 阶段的排序，I/O 开销没办法在框架里优化，所以尝试优化排序部分。比较简单的解决思路是不考虑实际应用，用一个大的 map 做合并。

   a. 一开始的方案是把 kv 先读到一个 `intermediate` 键值对数组里，之后调用 sort 根据 key 进行排序：

   ```go
   intermediate := []KeyValue{}
   
   sort.Sort(ByKey(intermediate))
   ```

   b. 后续修改为直接把 kv 读进一个 map 里，再对 map 里的 key 根据字母序进行排序：

   ```go
   intermediateMap := make(map[string][]string)
   ```

   这个方案减少了排序的开销，时间从完成 1GB 的 case9 用 600s 优化到完成全部测试耗时 437s。

### URLTop10

这部分实验就是基于 example 自己写一个 URLTop10 的 Map 函数和 Reduce 函数。

主要的目标是减少中间的 I/O 开销。实现的思路是分别给两轮 map 加一个 combine 函数，先减少一部分 map 传递给 reduce 的数据量。

在第一轮 Count 阶段，增加了 combine，在每个 map 输出的子集里对 <key, 1> 中 key 相同的求总数：

```go
combinedKvs := make([]KeyValue, 0)
for k, v := range clusteredKvs {
	combinedKvs = append(combinedKvs, KeyValue{Key: k, Value: strconv.Itoa(len(v))})
}
```

之后在 reduce 函数里把每个 map 文件里的相同 key 部分的 count 加和。这样减少了大量的中间结果文件读写开销。

做完这部分优化以后，测试时间从之前的 437s 优化到了 58s。

<img src="Screen Shot 2021-08-05 at 01.02.19.png" alt="Screen Shot 2021-08-05 at 01.02.19" style="zoom:50%;" align="left"/>

观察发现虽然大部分 case 都优化效果显著，但是所有数据量下的 case4 相比修改前会慢很多，查看数据分布发现 case4 主要是重复访问量很小的 url，最多也只有 3 次。所以 count 阶段对 map 数据合并不能减少很多中间结果数据量，反而增加了开销。

考虑在 top10 的 map 阶段再加一个 combine，这个 combine 直接对当前 map 函数的子结果求一次 top10，因为 round2 每个 map 函数里只处理 round1 一个 reduce 的输出，所以 key 和其他 map 是不重复的，reduce 再对每个 map 出的 top10 合并后的结果取 top10 不会影响正确性。得到最终的测试结果，相比第一次在 case4 都有优化，其他 case 因为 url 本来也不多，反而增加了一点开销，但可以接受。

