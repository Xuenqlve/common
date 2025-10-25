package chunk

import (
	"fmt"
	"strings"

	sql_tool "github.com/xuenqlve/common/sql"
)

const (
	lt  = "<"
	lte = "<="
	gt  = ">"
	gte = ">="
)

func ScanWhereSQL(chunk *Chunk, scanRange string, next bool) (where string, args []any) {
	where, args = toWhere(chunk, next)
	if where == "" {
		return scanRange, args
	} else if scanRange != "" {
		where = fmt.Sprintf("(%s) AND (%s)", where, scanRange)
	}
	where = sql_tool.RemoveRedundantParentheses(where)
	return
}

func toWhere(chunk *Chunk, next bool) (string, []any) {
	// 处理边界情况：没有Bounds
	if len(chunk.Bounds) == 0 {
		return "", nil
	}

	// 单列情况下的简化处理
	if len(chunk.Bounds) == 1 {
		return whereSimpleColumn(chunk, next)
	}
	// 多列情况下处理
	return whereComplexColumn(chunk, next)
}

func whereSimpleColumn(chunk *Chunk, next bool) (string, []any) {
	bound := chunk.Bounds[0]
	lowerSymbol := gte
	if next {
		lowerSymbol = gt
	}

	// 只有下界
	if bound.HasLower && !bound.HasUpper {
		return fmt.Sprintf("%s %s ?", sql_tool.ColumnName(bound.Column), lowerSymbol), []any{bound.Lower}
	}

	// 只有上界
	if !bound.HasLower && bound.HasUpper {
		return fmt.Sprintf("%s <= ?", sql_tool.ColumnName(bound.Column)), []any{bound.Upper}
	}

	// 同时有上下界且值相等
	if bound.HasLower && bound.HasUpper && bound.Lower == bound.Upper {
		return fmt.Sprintf("%s = ?", sql_tool.ColumnName(bound.Column)), []any{bound.Lower}
	}

	// 同时有上下界且值不等
	if bound.HasLower && bound.HasUpper {
		where := fmt.Sprintf("%s %s ? AND %s <= ?",
			sql_tool.ColumnName(bound.Column),
			lowerSymbol,
			sql_tool.ColumnName(bound.Column))
		return where, []any{bound.Lower, bound.Upper}
	}

	// 无上下界
	return "", nil
}

func whereComplexColumn(chunk *Chunk, next bool) (string, []any) {
	// 为不同类型的条件初始化切片，预分配合理的容量避免频繁扩容
	sameCondition := make([]string, 0, len(chunk.Bounds))  // 存储值相等的条件（Lower==Upper）
	lowerCondition := make([]string, 0, len(chunk.Bounds)) // 存储下界条件
	upperCondition := make([]string, 0, len(chunk.Bounds)) // 存储上界条件

	sameArgs := make([]any, 0, len(chunk.Bounds))    // 相等条件的参数
	lowerArgs := make([]any, 0, len(chunk.Bounds)*2) // 下界条件的参数（可能包含前置条件参数）
	upperArgs := make([]any, 0, len(chunk.Bounds)*2) // 上界条件的参数（可能包含前置条件参数）

	// 用于构建复合条件的前置条件
	preConditionForLower := make([]string, 0, len(chunk.Bounds))
	preConditionForUpper := make([]string, 0, len(chunk.Bounds))
	preConditionArgsForLower := make([]any, 0, len(chunk.Bounds))
	preConditionArgsForUpper := make([]any, 0, len(chunk.Bounds))

	// 第一阶段：处理上下界值相等的列（转换为等值条件）
	i := 0
	for ; i < len(chunk.Bounds); i++ {
		bound := chunk.Bounds[i]
		// 如果不同时具有上下界或上下界值不相等，则退出相等处理阶段
		if !(bound.HasLower && bound.HasUpper) {
			break
		}

		if bound.Lower != bound.Upper {
			break
		}

		// 处理相等条件
		sameCondition = append(sameCondition, fmt.Sprintf("%s = ?", sql_tool.ColumnName(bound.Column)))
		sameArgs = append(sameArgs, bound.Lower)
	}

	// 第二阶段：处理需要范围比较的列
	for ; i < len(chunk.Bounds); i++ {
		bound := chunk.Bounds[i]
		// 确定比较符号
		lowerSymbol := gt             // 下界默认使用 >
		upperSymbol := lt             // 上界默认使用 <
		if i == len(chunk.Bounds)-1 { // 最后一列的上界使用 <=
			upperSymbol = lte
			lowerSymbol = gte
			if next {
				lowerSymbol = gt
			}
		}

		// 处理下界条件
		if bound.HasLower {
			if len(preConditionForLower) > 0 {
				// 构建带前置条件的复合下界条件
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s ?)",
					strings.Join(preConditionForLower, " AND "),
					sql_tool.ColumnName(bound.Column),
					lowerSymbol))

				// 复制并合并前置条件参数和当前参数
				newArgs := make([]any, len(preConditionArgsForLower)+1)
				copy(newArgs, preConditionArgsForLower)
				newArgs[len(preConditionArgsForLower)] = bound.Lower
				lowerArgs = append(lowerArgs, newArgs...)
			} else {
				// 简单下界条件
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s ?)",
					sql_tool.ColumnName(bound.Column),
					lowerSymbol))
				lowerArgs = append(lowerArgs, bound.Lower)
			}

			// 更新前置条件，用于构建下一个列的条件
			preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = ?", sql_tool.ColumnName(bound.Column)))
			preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
		}

		// 处理上界条件
		if bound.HasUpper {
			if len(preConditionForUpper) > 0 {
				// 构建带前置条件的复合上界条件
				upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s ?)",
					strings.Join(preConditionForUpper, " AND "),
					sql_tool.ColumnName(bound.Column),
					upperSymbol))

				// 复制并合并前置条件参数和当前参数
				newArgs := make([]any, len(preConditionArgsForUpper)+1)
				copy(newArgs, preConditionArgsForUpper)
				newArgs[len(preConditionArgsForUpper)] = bound.Upper
				upperArgs = append(upperArgs, newArgs...)
			} else {
				// 简单上界条件
				upperCondition = append(upperCondition, fmt.Sprintf("(%s %s ?)",
					sql_tool.ColumnName(bound.Column),
					upperSymbol))
				upperArgs = append(upperArgs, bound.Upper)
			}

			// 更新前置条件，用于构建下一个列的条件
			preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = ?", sql_tool.ColumnName(bound.Column)))
			preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
		}
	}

	// 第三阶段：根据条件组合生成最终SQL和参数
	var where string
	var args []any

	// 根据不同情况组合SQL
	if len(sameCondition) == 0 {
		// 无相等条件时的处理
		if len(upperCondition) == 0 && len(lowerCondition) == 0 {
			// 没有任何条件
			return "", nil
		}

		if len(upperCondition) == 0 {
			// 只有下界条件
			return strings.Join(lowerCondition, " OR "), lowerArgs
		} else if len(lowerCondition) == 0 {
			// 只有上界条件
			return strings.Join(upperCondition, " OR "), upperArgs
		} else {
			// 同时有上下界条件，需要合并参数
			where = fmt.Sprintf("(%s) AND (%s)",
				strings.Join(lowerCondition, " OR "),
				strings.Join(upperCondition, " OR "))

			args = make([]any, len(lowerArgs)+len(upperArgs))
			copy(args, lowerArgs)
			copy(args[len(lowerArgs):], upperArgs)
			return where, args
		}
	} else {
		// 有相等条件时的处理
		sameSQL := strings.Join(sameCondition, " AND ")

		if len(upperCondition) == 0 && len(lowerCondition) == 0 {
			// 只有相等条件
			return sameSQL, sameArgs
		}

		if len(upperCondition) == 0 {
			// 相等条件加下界条件
			where = fmt.Sprintf("(%s) AND (%s)",
				sameSQL,
				strings.Join(lowerCondition, " OR "))

			args = make([]any, len(sameArgs)+len(lowerArgs))
			copy(args, sameArgs)
			copy(args[len(sameArgs):], lowerArgs)
			return where, args
		} else if len(lowerCondition) == 0 {
			// 相等条件加上界条件
			where = fmt.Sprintf("(%s) AND (%s)",
				sameSQL,
				strings.Join(upperCondition, " OR "))

			args = make([]any, len(sameArgs)+len(upperArgs))
			copy(args, sameArgs)
			copy(args[len(sameArgs):], upperArgs)
			return where, args
		} else {
			// 同时有相等、下界和上界条件
			where = fmt.Sprintf("(%s) AND (%s) AND (%s)",
				sameSQL,
				strings.Join(lowerCondition, " OR "),
				strings.Join(upperCondition, " OR "))

			totalLen := len(sameArgs) + len(lowerArgs) + len(upperArgs)
			args = make([]any, totalLen)
			n := copy(args, sameArgs)
			n += copy(args[n:], lowerArgs)
			copy(args[n:], upperArgs)
			return where, args
		}
	}
}
