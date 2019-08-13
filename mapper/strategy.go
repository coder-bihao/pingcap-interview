package mapper

import (
    _ "fmt"

    "structs"
)

// 分割策略接口
type SpiltStrategy interface {
    Dispatch(string, uint64, int, map[int]chan *structs.Record)
    Done(map[int]chan *structs.Record)
    Init()
}

// DJB hash
type SpiltStrategyWithDJB struct {}

// 根据DJB hash算法计算字符串  根据下游协程数量进行求余
func (s *SpiltStrategyWithDJB) Dispatch(line string, pos uint64, num int, taskQueue map[int]chan *structs.Record) {
    hash := uint64(0)

    for i := 0; i < len(line); i++ {
        hash = ((hash << 5) + hash) + uint64(line[i])
    }

    record := &structs.Record {
        Character : line,
        Pos : pos,
    }

    // 发送到对应的队列中
    taskQueue[int(hash % uint64(num))] <- record

    return
}

// do nothing
func (s *SpiltStrategyWithDJB) Done(taskQueue map[int]chan *structs.Record) {
	return
}

// do nothing
func (s *SpiltStrategyWithDJB) Init() {
	return
}

// DEK hash
type SpiltStrategyWithDEK struct {}

// 使用DEK算法计算line 根据下游协程数量进行求余
func (s *SpiltStrategyWithDEK) Dispatch(line string, pos uint64, num int, taskQueue map[int]chan *structs.Record) {
    hash := uint64(len(line))

    for i := 0; i < len(line); i++ {
        hash = ((hash << 5) ^ (hash >> 27)) ^ uint64(line[i])
    }

    record := &structs.Record {
        Character : line,
        Pos : pos,
    }

    // 发送到对应的队列中
    taskQueue[int(hash % uint64(num))] <- record

    return
}

// do nothing
func (s *SpiltStrategyWithDEK) Done(taskQueue map[int]chan *structs.Record) {
	return
}

// do nothing
func (s *SpiltStrategyWithDEK) Init() {
	return
}

// mem duplicate
type SpiltStrategyWithMem struct {
    dedup map[string]*structs.RecordDedup
}

// 在内存中进行去重 并不真正分发
func (s *SpiltStrategyWithMem) Init() {
    s.dedup = make(map[string]*structs.RecordDedup)
}


// 在内存中进行去重 并不真正分发
func (s *SpiltStrategyWithMem) Dispatch(line string, pos uint64, num int, taskQueue map[int]chan *structs.Record) {
    // 存在则对Repeat进行+1操作
    if r, ok := s.dedup[line]; ok {
        r.Repeat = r.Repeat + 1

        return
    }

    // 不存在则创建一个记录
    record := &structs.RecordDedup {
        Pos : pos,
        Repeat : 1,
    }
    s.dedup[line] = record

    return
}

// 在内存中对map遍历 获取到最小pos位的数据 并真正分发
func (s *SpiltStrategyWithMem) Done(taskQueue map[int]chan *structs.Record) {
    minPos := uint64(0)
    line := ""

    for k, v := range s.dedup {
        if v.Repeat > 1 {
            continue
        }

        if v.Pos < minPos || minPos == 0 {
            minPos = v.Pos
            line = k
        }
    }

    if minPos != 0 {
        record := &structs.Record {
            Character : line,
            Pos : minPos,
        }

        // 只有一条记录 发送给任意协程即可
        taskQueue[1] <- record
    }

    return
}
