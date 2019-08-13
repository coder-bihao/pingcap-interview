// reducer包 
// 用于分析小文件内容
package reducer

import (
    "fmt"
    "os"
    "bufio"
    "sync"
    "path/filepath"
    "strings"
    "strconv"

    "structs"
)

var once sync.Once
var reducer *Reducer

// 分析小文件类 
// 加载小文件到内存中进行统计计数
type Reducer struct {
    // 需要遍历分析的文件夹路径
    Dir string

    // 内存中去重使用的hash map
    dedup map[string]*structs.RecordDedup
}

// 创建reducer实例 singleton
func NewReducer(dir string) *Reducer {
    once.Do(func() {
        reducer = &Reducer {
            Dir : dir,
            dedup : make(map[string]*structs.RecordDedup),
        }

    })

    return reducer
}

func (r *Reducer) Reduce() (string, uint64) {
    line := ""
    minPos := uint64(0)

    // 遍历文件夹下的文件 加载到内存中
    filepath.Walk(r.Dir, func(path string, info os.FileInfo, err error) error {
        fmt.Printf("reduce| analyse file[%s] begin.\n", path)
        inFileLine := ""
        inFileMinPos := uint64(0)
        f, _ := os.Open(path)
        defer f.Close()

        buffer := bufio.NewReader(f)

        // 读文件加载内存
        for {
            line, _, err := buffer.ReadLine()

            if err != nil {
                break
            }

            str, pos := r.spiltLine(string(line))

            // 存在则对Repeat进行+1操作
            if record, ok := r.dedup[str]; ok {
                record.Repeat = record.Repeat + 1
                //fmt.Printf("reduce| debug record repeat[%d].\n", record.Repeat)
                continue
            }

            record := &structs.RecordDedup {
                Pos : pos,
                Repeat : 1,
            }

            // 不存在则创建一个记录
            r.dedup[str] = record
        }

        // 遍历hash map 查找最小pos的line
        for k, v := range r.dedup {
            //fmt.Printf("reduce| debug file[%s] k[%v] v.Repeat[%v]. v.Pos[%v]\n", path, k, v.Repeat, v.Pos)
            if v.Repeat > 1 {
                continue
            }

            // 计算本次文件中的最小位置 for trace
            if v.Pos < inFileMinPos || inFileMinPos == 0 {
                inFileMinPos = v.Pos
                inFileLine = k
            }

            if v.Pos < minPos || minPos == 0 {
                minPos = v.Pos
                line = k
            }
        }

        if inFileMinPos != 0 {
            fmt.Printf("reduce| analyse file[%s] end, line[%s] minPos[%v] in this file.\n", path, inFileLine, inFileMinPos)
        } else {
            fmt.Printf("reduce| analyse file[%s] end, all line duplicate.\n", path)
        }

        // 清理hash map 
        r.dedup = make(map[string]*structs.RecordDedup)

        return nil
    })

    return line, minPos
}

// 分割字符串 \t分割
func (r *Reducer) spiltLine(line string) (string, uint64) {
    spiltList := strings.Split(line, "\t")
    pos, _ := strconv.ParseUint(spiltList[1], 10, 64)

    return spiltList[0], pos
}
