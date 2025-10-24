package sql

import "strings"

// RemoveRedundantParentheses 移除SQL条件中冗余的括号
func RemoveRedundantParentheses(sql string) string {
	if sql == "" {
		return sql
	}

	// 解析SQL为AST
	root := parseExpression(sql)

	// 遍历AST生成格式化后的SQL
	return formatExpression(root, nil)
}

// 表达式节点类型
const (
	NodeTypeAND       = "AND"
	NodeTypeOR        = "OR"
	NodeTypeCondition = "CONDITION"
)

// ExprNode 表示表达式节点
type ExprNode struct {
	Type     string      // 节点类型: AND, OR, CONDITION
	Value    string      // 条件值（对于CONDITION类型）
	Children []*ExprNode // 子节点（对于AND/OR类型）
}

// parseExpression 将SQL条件解析为表达式树
func parseExpression(expr string) *ExprNode {
	expr = strings.TrimSpace(expr)

	// 递归深度预处理，预先处理掉多层括号
	expr = deepRemoveParentheses(expr)

	// 尝试分割顶层OR操作符（优先级低）
	orParts := splitByTopLevelOperator(expr, "OR")
	if len(orParts) > 1 {
		node := &ExprNode{
			Type:     NodeTypeOR,
			Children: make([]*ExprNode, 0, len(orParts)),
		}

		for _, part := range orParts {
			// 递归处理每个部分
			node.Children = append(node.Children, parseExpression(part))
		}

		return node
	}

	// 尝试分割顶层AND操作符（优先级高）
	andParts := splitByTopLevelOperator(expr, "AND")
	if len(andParts) > 1 {
		node := &ExprNode{
			Type:     NodeTypeAND,
			Children: make([]*ExprNode, 0, len(andParts)),
		}

		for _, part := range andParts {
			// 递归处理每个部分
			node.Children = append(node.Children, parseExpression(part))
		}

		return node
	}

	// 如果不能分割，则是一个基本条件
	return &ExprNode{
		Type:  NodeTypeCondition,
		Value: expr,
	}
}

// deepRemoveParentheses 深度递归移除表达式中的冗余括号
func deepRemoveParentheses(expr string) string {
	expr = strings.TrimSpace(expr)

	// 处理空表达式
	if expr == "" {
		return expr
	}

	// 如果不是被括号包围，尝试拆分处理子表达式
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		// 尝试分割顶层操作符，并递归处理每个部分
		orParts := splitByTopLevelOperator(expr, "OR")
		if len(orParts) > 1 {
			for i, part := range orParts {
				orParts[i] = deepRemoveParentheses(part)
			}
			return strings.Join(orParts, " OR ")
		}

		andParts := splitByTopLevelOperator(expr, "AND")
		if len(andParts) > 1 {
			for i, part := range andParts {
				andParts[i] = deepRemoveParentheses(part)
			}
			return strings.Join(andParts, " AND ")
		}

		return expr
	}

	// 如果是被括号包围，先移除最外层括号
	inner := expr[1 : len(expr)-1]
	if !isBalancedParentheses(inner) {
		return expr // 括号不匹配，不做处理
	}

	// 递归处理内部表达式
	processed := deepRemoveParentheses(inner)

	// 特殊处理: 对于已有操作符的表达式，根据操作符处理括号
	if hasTopLevelOperator(processed) {
		// 对于OR表达式，根据周围环境判断是否保留括号
		if strings.Contains(processed, " OR ") {
			orParts := splitByTopLevelOperator(processed, "OR")
			for i, part := range orParts {
				orParts[i] = deepRemoveParentheses(part)
			}
			// OR表达式通常需要保留括号
			return processed
		}

		// 对于AND表达式，通常可以移除外围括号
		if strings.Contains(processed, " AND ") {
			andParts := splitByTopLevelOperator(processed, "AND")
			for i, part := range andParts {
				andParts[i] = deepRemoveParentheses(part)
			}
			return processed
		}
	}

	// 简单条件，可以安全移除最外层括号
	return processed
}

// formatExpression 根据表达式树生成格式化SQL条件
func formatExpression(node *ExprNode, parent *ExprNode) string {
	if node == nil {
		return ""
	}

	switch node.Type {
	case NodeTypeCondition:
		// 基本条件，移除冗余括号
		return removeOutermostParentheses(node.Value)

	case NodeTypeAND:
		parts := make([]string, 0, len(node.Children))
		for _, child := range node.Children {
			formatted := formatExpression(child, node)
			parts = append(parts, formatted)
		}
		result := strings.Join(parts, " AND ")

		// AND表达式通常不需要括号
		return result

	case NodeTypeOR:
		parts := make([]string, 0, len(node.Children))
		for _, child := range node.Children {
			formatted := formatExpression(child, node)

			// 如果子节点是AND，则在OR条件下需要括号以保持优先级
			if child.Type == NodeTypeAND {
				formatted = "(" + formatted + ")"
			}

			parts = append(parts, formatted)
		}
		result := strings.Join(parts, " OR ")

		// 如果父节点是AND，则OR条件需要括号以保持优先级
		if parent != nil && parent.Type == NodeTypeAND {
			return "(" + result + ")"
		}

		return result
	}

	return ""
}

// removeOutermostParentheses 移除表达式最外层的冗余括号
func removeOutermostParentheses(expr string) string {
	expr = strings.TrimSpace(expr)

	// 如果不是被括号包围，直接返回
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return expr
	}

	// 循环移除外层括号
	for len(expr) >= 2 && expr[0] == '(' && expr[len(expr)-1] == ')' {
		// 检查这对括号是否匹配（是否为一对）
		inner := expr[1 : len(expr)-1]
		if !isBalancedParentheses(inner) {
			break
		}

		// 如果内部包含操作符，可能需要保留括号，此时不继续移除
		if hasOperator(inner) {
			break
		}

		expr = inner
	}

	return expr
}

// hasOperator 检查字符串是否包含操作符
func hasOperator(expr string) bool {
	return strings.Contains(expr, " AND ") || strings.Contains(expr, " OR ")
}

// hasTopLevelOperator 判断表达式是否含有顶层操作符
func hasTopLevelOperator(expr string) bool {
	parenCount := 0
	tokens := strings.Fields(expr)

	for _, token := range tokens {
		// 统计括号
		for _, ch := range token {
			if ch == '(' {
				parenCount++
			} else if ch == ')' {
				parenCount--
			}
		}

		// 只在顶层（括号外）检查操作符
		if parenCount == 0 && (token == "AND" || token == "OR") {
			return true
		}
	}

	return false
}

// splitByTopLevelOperator 按顶层操作符分割表达式
func splitByTopLevelOperator(expr string, operator string) []string {
	var parts []string
	var currentPart strings.Builder

	parenCount := 0
	inQuote := false
	tokens := strings.Fields(expr)

	for _, token := range tokens {
		// 检查是否为目标操作符且在顶层（括号外）
		if parenCount == 0 && !inQuote && token == operator {
			// 保存当前部分并重置
			if currentPart.Len() > 0 {
				parts = append(parts, strings.TrimSpace(currentPart.String()))
				currentPart.Reset()
			}
			continue
		}

		// 处理字符串里的引号和括号
		for _, ch := range token {
			if ch == '\'' {
				inQuote = !inQuote
			} else if !inQuote {
				if ch == '(' {
					parenCount++
				} else if ch == ')' {
					parenCount--
				}
			}
		}

		// 添加到当前部分
		if currentPart.Len() > 0 {
			currentPart.WriteString(" ")
		}
		currentPart.WriteString(token)
	}

	// 添加最后一部分
	if currentPart.Len() > 0 {
		parts = append(parts, strings.TrimSpace(currentPart.String()))
	}

	return parts
}

// wrapConditionsWithParentheses 确保每个条件都有外层括号
// 用于处理 OR 组合条件时，保证每个子条件都被括号包围
// 参数:
//   - conditions: 待处理的条件字符串数组
//
// 返回值:
//   - 处理后的条件字符串数组，每个元素都带有括号
func wrapConditionsWithParentheses(conditions []string) []string {
	wrapped := make([]string, 0, len(conditions))
	for _, cond := range conditions {
		// 如果不是以括号开始，则添加括号
		if !strings.HasPrefix(cond, "(") || !strings.HasSuffix(cond, ")") {
			cond = "(" + cond + ")"
		}
		wrapped = append(wrapped, cond)
	}
	return wrapped
}

// isBalancedParentheses 检查括号是否匹配
func isBalancedParentheses(s string) bool {
	var count int
	for _, ch := range s {
		if ch == '(' {
			count++
		} else if ch == ')' {
			count--
		}
		if count < 0 {
			return false
		}
	}
	return count == 0
}
