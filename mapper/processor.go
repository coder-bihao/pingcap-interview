package mapper

import (
    "os"
    "fmt"
    "sync"
    "time"
    "strconv"
    "container/list"

    "structs"
)

// 消费Hash后的数据类 将数据进行落盘
type Processor struct {
    // 互斥锁
    sync.Mutex

    // 启停channel
    stopCh chan struct {}

    // 数据通道 绑定到固定Processor
    queue chan *structs.Record

    // 内部buffer 
    // 防止阻塞channel
    // 通过checkpoint机制对数据进行落盘 减少io次数
    buffer *list.List

    // buffer阈值 超过阈值则进行数据dump
    Threshold int

    // 定时器时间
    time int

    // 轮次
    Round int

    // 协程ID
    ID int

    // 落盘文件目录
    Dir string
}

// 创建processor实例 
func NewProcessor(q chan *structs.Record, id int, dir string) *Processor {
    p := &Processor {
        queue : q,
        ID : id,
        buffer : list.New(),
        Threshold : 10000,
        time : 5,
        Dir : dir,
    }

    return p
}

// 处理协程入口
func (p *Processor) Run(round int) {
    p.SetRound(round)

    for {
        timer := time.After(time.Second * time.Duration(p.time))
        select {
        case t := <-p.queue:
            p.InsertCache(t)
        case <-timer:
            fmt.Printf("after timer dump.\n")
            p.Dump()
        case <-p.stopCh:
            fmt.Printf("after stop dump.\n")
            // graceful exit
            // p.Lock()
            left := len(p.queue)

            for idx := 0; idx < left; idx++ {
                c := <-p.queue
                p.InsertCache(c)
            }

            // p.UnLock()
            p.Dump()
            wg.Done()

            return
        }
    }
}

// 设置当前轮次 用于区分文件
func (p *Processor) SetRound(round int) {
    p.Round = round
}

// 插入buffer
func (p *Processor) InsertCache(r *structs.Record) {
    p.buffer.PushBack(r)

    if p.buffer.Len() > p.Threshold {
        fmt.Printf("over threshold dump.\n")
        p.Dump()
    }

    return
}

// 数据落盘
// 协程id + round id保证文件唯一性
func (p *Processor) Dump() {
    fileName := p.Dir + "/" + strconv.Itoa(p.ID) + "_" + strconv.Itoa(p.Round)
    fmt.Printf("processor| debug id[%d] round[%d] dump file[%s].\n", p.ID, p.Round, fileName)

    file, _ := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
    defer file.Close()

    for e := p.buffer.Front(); e != nil; e = e.Next() {
        line := e.Value.(*structs.Record).Character + "\t" + strconv.FormatUint(e.Value.(*structs.Record).Pos, 10) + "\n"
        //fmt.Printf("processor| debug line[%s] write to file[%s].\n", line, fileName)
        file.WriteString(line)
    }

    // 清理buffer数据
    p.buffer.Init()

    return
}

// 设置退出的通知channel
func (p *Processor) SetStopCh(ch chan struct{}) {
    p.stopCh = ch
}
