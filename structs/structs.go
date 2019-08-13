// structs包
// 内部结构体
package structs

// 单行数据 原始字符 + 字符所在位置
type Record struct {
    Character string
    Pos uint64
}

// 用于去重使用结构体
type RecordDedup struct {
    Pos uint64
    Repeat uint64
}
