package ddl_parser

import (
	"fmt"
)

// 全局实例，提供便捷的转换方法
var GlobalTransformer = NewStatementTransformer()

type TransformerOption interface {
	Transformer(src Statement) (bool, Statement, error)
}

// StatementTransformer 提供不同数据库Statement之间的转换功能
type StatementTransformer struct {
	transformers map[string]map[string]TransformerOption
}

// NewStatementTransformer 创建新的Statement转换器
func NewStatementTransformer() *StatementTransformer {
	return &StatementTransformer{
		transformers: make(map[string]map[string]TransformerOption),
	}
}

// RegisterTransformer 注册转换器
func (st *StatementTransformer) RegisterTransformer(fromType, toType string, transformer TransformerOption) {
	if st.transformers[fromType] == nil {
		st.transformers[fromType] = make(map[string]TransformerOption)
	}
	st.transformers[fromType][toType] = transformer
}

// Transform 执行Statement转换
func (st *StatementTransformer) Transform(src Statement, fromType, toType string) (bool, Statement, error) {
	if st.transformers[fromType] == nil {
		return false, nil, fmt.Errorf("unsupported source type: %s", fromType)
	}

	transformer, exists := st.transformers[fromType][toType]
	if !exists {
		return false, nil, fmt.Errorf("unsupported transformation from %s to %s", fromType, toType)
	}

	return transformer.Transformer(src)
}
