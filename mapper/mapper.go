// mapper包 
// 用于分割文件
package mapper

import (
    "os"
    "fmt"
    "sync"
    "errors"
    "bufio"
    "strings"
    "strconv"
    "path/filepath"

    "structs"
)

var spilter *Spilt
var once sync.Once
var wg sync.WaitGroup

// 分割文件类 用于将输入文件分割为小文件
// 加载文件 并将字符串进行hash至固定的队列中交给下游处理
// 由于hash不均匀或者原文件中有大量相同字符串 可能有多轮处理
type Spilt struct {
    // 互斥锁
    sync.Mutex

    // 启停channel
    StopCh chan struct {}

    // 数据队列 队列绑定到固定的goroutine处理
    TaskQueue map[int]chan *structs.Record

    // 下游处理goroutine
    Processors map[int]*Processor

    // processor个数 
    ProcessorNum int

    // 大文件路径
    BigFilePath string

    // 分割后的小文件存储路径
    SpiltedFilesPath string

    // 分割后的文件最大容量 单位GB
    MaxSize int

    // 使用的分割策略 md5 hash/elf hash/内存hash表去重
    // 若再hash仍然大小超过阈值 则认为是大量重复字符
    spiltStrategy SpiltStrategy

    // 最大轮次
    Round int

    // 任务是否全部完成标记
    // 需要check下游goroutine生成的文件大小是否满足设置的阈值
    IsComplete bool
}

// 创建spilt实例 singleton
func NewSpilter(bigFilePath string, spiltedFilesPath string, processorNum int, maxSize int) *Spilt {
    once.Do(func() {
        spilter = &Spilt {
            TaskQueue : make(map[int]chan *structs.Record),
            Processors : make(map[int]*Processor),
            ProcessorNum : processorNum,
            MaxSize : maxSize,
            BigFilePath : bigFilePath,
            SpiltedFilesPath : spiltedFilesPath,
            Round : 3,
            IsComplete : false,
        }

        for idx := 0; idx < spilter.ProcessorNum; idx++ {
            ch := make(chan *structs.Record, 10000)
            spilter.TaskQueue[idx]  = ch
            spilter.Processors[idx] = NewProcessor(ch, idx, spilter.SpiltedFilesPath)
        }
    })

    return spilter
}

// 启动下游processor 消费队列数据
func (s *Spilt) StartProcessor(round int) {
    fmt.Printf("mapper| start processor round[%d] begin.\n", round)
    s.Lock()
    s.StopCh = make(chan struct{})

    // 启动processor
    for idx := 0; idx < s.ProcessorNum; idx++ {
        p, _ := s.Processors[idx]
        p.SetStopCh(s.StopCh)
        go p.Run(round)
    }

    wg.Add(s.ProcessorNum)

    s.Unlock()
}

// 停止下游processor 
func (s *Spilt) ShutDownProcessor() {
    fmt.Printf("mapper| shut down processors.\n")
    s.Lock()

    close(s.StopCh)
    wg.Wait()

    s.Unlock()
}

// 根据轮次设置分割策略
// strategy0 : md5 hash
// strategy1 : elf hash
// strategy2 : mem duplicate
func (s *Spilt) setSpiltStrategy (round int) error {
    fmt.Printf("mapper| set round[%d] strategy.\n", round)
    switch round {
    case 0:
        s.spiltStrategy = new(SpiltStrategyWithDJB)
    case 1:
        s.spiltStrategy = new(SpiltStrategyWithDEK)
    case 2:
        s.spiltStrategy = new(SpiltStrategyWithMem)
    default:
        return errors.New("unsupport spilt strategy")
    }

    s.spiltStrategy.Init()

    return nil
}

// 逐行读取大文件内容 并将数据Push到队列
// 可能存在多轮
func (s *Spilt) Map() (bool, error) {
    for i := 0; i < s.Round; i++ {
        // step1.启动processor消费队列数据
        s.StartProcessor(i)

        // step2.设置分割策略
        err := s.setSpiltStrategy(i)

        if err != nil {
            return false, err
        }

        // step3.获取大文件 并根据策略进行分割
        err = s.SpiltBigFiles(i)

        if err != nil {
            return false, err
        }

        // step4.停止processor消费协程
        s.ShutDownProcessor()

        if bigFiles := s.selectBigSpiltedFiles(s.SpiltedFilesPath); len(bigFiles) == 0 {
            s.IsComplete = true
            break
        }
    }

    return s.IsComplete, nil
}

// 获取大文件 并根据策略进行分割
func (s *Spilt) SpiltBigFiles(round int) error {
    fmt.Printf("mapper| spilt big files round[%d] begin.\n", round)
    filePaths := make([]string, 0)

    // 获取需要切割的文件路径
    switch round {
    case 0:
        if s.isFileExists(s.BigFilePath) {
            filePaths = append(filePaths, s.BigFilePath)
        } else {
            return errors.New("path not exists")
        }
    case 1, 2:
        if s.isFileExists(s.SpiltedFilesPath) {
            filePaths = s.selectBigSpiltedFiles(s.SpiltedFilesPath)
        } else {
            return errors.New("path not exists")
        }
    default:
        return errors.New("not support round")
    }

    // 逐行读取大文件 并生成record
    // 根据策略进行分发
    for _, file := range filePaths {
        f, _ := os.Open(file)
        defer f.Close()

        r := bufio.NewReader(f)
        pos := uint64(1)

        for {
            line, _, err := r.ReadLine()

            if err != nil {
                break
            }

            // 读取的非原始文件
            if round > 0 {
                str, pos := s.spiltLine(string(line))
                s.spiltStrategy.Dispatch(str, pos, s.ProcessorNum, s.TaskQueue)
            }

            //fmt.Printf("mapper| debug line[%s] pos[%v] dispatch.\n", string(line), pos)
            s.spiltStrategy.Dispatch(string(line), pos, s.ProcessorNum, s.TaskQueue)
            pos = pos + 1
        }
    }

    // 考虑内存去重策略 需要显示调用Done
    s.spiltStrategy.Done(s.TaskQueue)

    // 移除上一轮次的大文件
    if round > 0 && len(filePaths) > 0 {
        for _, file := range filePaths {
            fmt.Printf("mapper| remove file[%s].", file)
            os.Remove(file)
        }
    }

    return nil
}

// 判断文件、目录是否存在
func (s *Spilt) isFileExists(path string) bool {
    _, err := os.Stat(path)

    if err == nil {
        return true
    }

    if os.IsNotExist(err) {
        return false
    }

    return false
}

// 选择目录中超过阈值大小的文件
func (s *Spilt) selectBigSpiltedFiles(path string) []string {
    ret := make([]string, 0)
    filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
        if s.isOverMaxSize(path) {
            ret = append(ret, path)
        }

        return nil
    })

    return ret
}

// 判断文件是否超过最大阈值
func (s *Spilt) isOverMaxSize(path string) bool {
    f, err := os.Open(path)
    defer f.Close()

    if err != nil {
        return false
    }

    size, _ := f.Seek(0, os.SEEK_END)
    f.Seek(0, os.SEEK_SET)

    if int(size/1024/1024/1024) > s.MaxSize {
        return true
    }

    return false
}

// 分割字符串 \t分割
func (s *Spilt) spiltLine(line string) (string, uint64) {
    spiltList := strings.Split(line, "\t")
    pos, _ := strconv.ParseUint(spiltList[1], 10, 64)

    return spiltList[0], pos
}
