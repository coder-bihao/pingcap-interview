// main包
// 用于实现有限内存大文件的数据分析
package main

import (
    "fmt"

    "mapper"
    "reducer"
)

func main() {
    bigFilePath := "/Users/haobi/interview/pingcap/test/bigfile.txt"
    spiltedFilesPath := "/Users/haobi/interview/pingcap/test/splited"

    // step1.对大文件进行切割 map阶段
    mapper := mapper.NewSpilter(bigFilePath, spiltedFilesPath, 20, 5)
    ok, err := mapper.Map()

    if err != nil || !ok {
        fmt.Printf("main| map failed, err[%v] ret[%v].\n", err, ok)
        return
    }

    fmt.Println("main| map phase end.")

    // step2.对生成的小文件load到内存进行分析 reduce阶段
    reducer := reducer.NewReducer(spiltedFilesPath)
    line, pos := reducer.Reduce()

    if line == "" {
        fmt.Println("main| all line duplicate.")
    } else {
        fmt.Printf("main| result line[%s] pos[%v].\n", line, pos)
    }

    return
}
