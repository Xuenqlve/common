package where

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

func WhereFilter(src map[string]interface{}, where string) (bool, error) {
	if where == "" {
		return true, nil
	}

	// 输入长度检查
	if len(where) > MaxInputLength {
		return false, fmt.Errorf("input length %d exceeds maximum %d", len(where), MaxInputLength)
	}

	// 先从缓存中查找已解析的表达式树
	if cached, ok := ExprCache.Load(where); ok {
		cachedExpr := cached.(*CachedExprNode)
		if cachedExpr.Error != nil {
			// 缓存的是解析错误，回退到简单的whereFilter
			return whereFilter(src, where), nil
		}

		// 使用缓存的表达式树直接求值
		// 创建带超时的上下文用于求值
		ctx, cancel := context.WithTimeout(context.Background(), ParseTimeout)
		defer cancel()

		parseCtx := &ParseContext{
			depth:      0,
			maxDepth:   MaxRecursionDepth,
			tokenCount: 0,
			maxTokens:  MaxTokenCount,
			ctx:        ctx,
			cancel:     cancel,
		}

		result, err := evaluateExpressionWithContext(src, cachedExpr.Root, parseCtx)
		if err != nil {
			// 检查是否是超时错误
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return false, fmt.Errorf("evaluation timeout: %w", err)
			}
			// 其他错误：回退到简单的whereFilter
			return whereFilter(src, where), nil
		}

		return result, nil
	}

	// 缓存中没有找到，需要解析where条件
	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), ParseTimeout)
	defer cancel()

	// 创建解析上下文
	parseCtx := &ParseContext{
		depth:      0,
		maxDepth:   MaxRecursionDepth,
		tokenCount: 0,
		maxTokens:  MaxTokenCount,
		ctx:        ctx,
		cancel:     cancel,
	}

	// 词法分析：将字符串解析为tokens
	tokens, err := tokenizeWithContext(where, parseCtx)
	if err != nil {
		// 缓存解析错误
		ExprCache.Store(where, &CachedExprNode{Root: nil, Error: err})
		// 检查是否是超时错误
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false, fmt.Errorf("tokenization timeout: %w", err)
		}
		// 其他错误：回退到简单的whereFilter
		return whereFilter(src, where), nil
	}

	if len(tokens) == 0 {
		// 缓存空结果
		ExprCache.Store(where, &CachedExprNode{Root: nil, Error: nil})
		return true, nil
	}

	// 语法分析：构建表达式树
	root, err := parseExpressionWithContext(tokens, parseCtx)
	if err != nil {
		// 缓存解析错误
		ExprCache.Store(where, &CachedExprNode{Root: nil, Error: err})
		// 检查是否是超时错误
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false, fmt.Errorf("parsing timeout: %w", err)
		}
		// 其他错误：回退到简单的whereFilter
		return whereFilter(src, where), nil
	}

	// 缓存成功解析的表达式树
	ExprCache.Store(where, &CachedExprNode{Root: root, Error: nil})

	// 求值：递归计算表达式树的值
	result, err := evaluateExpressionWithContext(src, root, parseCtx)
	if err != nil {
		// 检查是否是超时错误
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false, fmt.Errorf("evaluation timeout: %w", err)
		}
		// 其他错误：回退到简单的whereFilter
		return whereFilter(src, where), nil
	}

	return result, nil
}

// CachedExprNode 缓存的表达式节点，包含解析结果和错误信息
type CachedExprNode struct {
	Root  *ExprNode
	Error error
}

// ExprCache 表达式缓存，使用 sync.Map 实现线程安全的缓存
var ExprCache sync.Map

// ClearExprCache 清空表达式缓存
func ClearExprCache() {
	ExprCache.Range(func(key, value interface{}) bool {
		ExprCache.Delete(key)
		return true
	})
}

// GetExprCacheSize 获取缓存中的条目数量
func GetExprCacheSize() int {
	count := 0
	ExprCache.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// ExprNode 表达式树节点
type ExprNode struct {
	Type      NodeType
	Condition string    // 条件表达式字符串
	Operator  string    // 逻辑操作符 ("and", "or")
	Left      *ExprNode // 左子节点
	Right     *ExprNode // 右子节点
}

type NodeType int

const (
	NodeCondition NodeType = iota // 条件节点
	NodeOperator                  // 操作符节点
)

// Token 表示解析后的词法单元
type Token struct {
	Type  TokenType
	Value string
}

type TokenType int

const (
	TokenCondition  TokenType = iota // 条件表达式 (例如 "age > 18")
	TokenAnd                         // AND 操作符
	TokenOr                          // OR 操作符
	TokenLeftParen                   // 左括号 (
	TokenRightParen                  // 右括号 )
)

// ParseContext 解析上下文，用于跟踪递归深度和资源使用
type ParseContext struct {
	depth      int
	maxDepth   int
	tokenCount int
	maxTokens  int
	ctx        context.Context
	cancel     context.CancelFunc
}

// 安全限制常量
const (
	MaxRecursionDepth   = 100             // 最大递归深度
	MaxInputLength      = 10000           // 最大输入长度
	MaxTokenCount       = 1000            // 最大token数量
	MaxParenthesesDepth = 50              // 最大括号嵌套深度
	ParseTimeout        = 5 * time.Second // 解析超时时间
)

// tokenizeWithContext 安全的词法分析器：将 where 字符串解析为 tokens
func tokenizeWithContext(where string, parseCtx *ParseContext) ([]Token, error) {
	// 检查上下文是否已取消
	select {
	case <-parseCtx.ctx.Done():
		return nil, parseCtx.ctx.Err()
	default:
	}

	var tokens []Token
	where = strings.TrimSpace(where)

	i := 0
	parenDepth := 0

	for i < len(where) {
		// 检查上下文是否已取消（每100次迭代检查一次）
		if i%100 == 0 {
			select {
			case <-parseCtx.ctx.Done():
				return nil, parseCtx.ctx.Err()
			default:
			}
		}

		// 检查token数量限制
		if len(tokens) >= parseCtx.maxTokens {
			return nil, fmt.Errorf("token count exceeded maximum: %d", parseCtx.maxTokens)
		}

		// 边界检查
		if i >= len(where) {
			break
		}

		// 跳过空白字符
		if where[i] == ' ' || where[i] == '\t' || where[i] == '\n' {
			i++
			continue
		}

		// 左括号
		if where[i] == '(' {
			parenDepth++
			if parenDepth > MaxParenthesesDepth {
				return nil, fmt.Errorf("parentheses depth exceeded maximum: %d", MaxParenthesesDepth)
			}
			tokens = append(tokens, Token{Type: TokenLeftParen, Value: "("})
			i++
			continue
		}

		// 右括号
		if where[i] == ')' {
			parenDepth--
			if parenDepth < 0 {
				return nil, fmt.Errorf("unmatched right parenthesis")
			}
			tokens = append(tokens, Token{Type: TokenRightParen, Value: ")"})
			i++
			continue
		}

		// 检查 AND 操作符（不区分大小写）
		if i+3 <= len(where) && strings.ToLower(where[i:i+3]) == "and" {
			// 确保 and 前后是空格或括号
			prevOk := i == 0 || where[i-1] == ' ' || where[i-1] == '\t' || where[i-1] == ')'
			nextOk := i+3 >= len(where) || where[i+3] == ' ' || where[i+3] == '\t' || where[i+3] == '('
			if prevOk && nextOk {
				tokens = append(tokens, Token{Type: TokenAnd, Value: "and"})
				i += 3
				continue
			}
		}

		// 检查 OR 操作符（不区分大小写）
		if i+2 <= len(where) && strings.ToLower(where[i:i+2]) == "or" {
			// 确保 or 前后是空格或括号
			prevOk := i == 0 || where[i-1] == ' ' || where[i-1] == '\t' || where[i-1] == ')'
			nextOk := i+2 >= len(where) || where[i+2] == ' ' || where[i+2] == '\t' || where[i+2] == '('
			if prevOk && nextOk {
				tokens = append(tokens, Token{Type: TokenOr, Value: "or"})
				i += 2
				continue
			}
		}

		// 读取条件表达式（直到遇到 AND、OR 或括号）
		start := i
		for i < len(where) {
			if i >= len(where) || where[i] == '(' || where[i] == ')' {
				break
			}
			// 检查是否遇到 AND
			if i+3 <= len(where) && strings.ToLower(where[i:i+3]) == "and" {
				if i > 0 && (where[i-1] == ' ' || where[i-1] == '\t') {
					nextOk := i+3 >= len(where) || where[i+3] == ' ' || where[i+3] == '\t' || where[i+3] == '('
					if nextOk {
						break
					}
				}
			}
			// 检查是否遇到 OR
			if i+2 <= len(where) && strings.ToLower(where[i:i+2]) == "or" {
				if i > 0 && (where[i-1] == ' ' || where[i-1] == '\t') {
					nextOk := i+2 >= len(where) || where[i+2] == ' ' || where[i+2] == '\t' || where[i+2] == '('
					if nextOk {
						break
					}
				}
			}
			i++
		}

		condition := strings.TrimSpace(where[start:i])
		if condition != "" {
			tokens = append(tokens, Token{Type: TokenCondition, Value: condition})
		}
	}

	// 检查括号是否匹配
	if parenDepth != 0 {
		return nil, fmt.Errorf("unmatched parentheses: depth %d", parenDepth)
	}

	return tokens, nil
}

// tokenize 词法分析器：将 where 字符串解析为 tokens（保留向后兼容性）
func tokenize(where string) []Token {
	var tokens []Token
	where = strings.TrimSpace(where)

	i := 0
	for i < len(where) {
		// 跳过空白字符
		if where[i] == ' ' || where[i] == '\t' || where[i] == '\n' {
			i++
			continue
		}

		// 左括号
		if where[i] == '(' {
			tokens = append(tokens, Token{Type: TokenLeftParen, Value: "("})
			i++
			continue
		}

		// 右括号
		if where[i] == ')' {
			tokens = append(tokens, Token{Type: TokenRightParen, Value: ")"})
			i++
			continue
		}

		// 检查 AND 操作符（不区分大小写）
		if i+3 <= len(where) && strings.ToLower(where[i:i+3]) == "and" {
			// 确保 and 前后是空格或括号
			prevOk := i == 0 || where[i-1] == ' ' || where[i-1] == '\t' || where[i-1] == ')'
			nextOk := i+3 >= len(where) || where[i+3] == ' ' || where[i+3] == '\t' || where[i+3] == '('
			if prevOk && nextOk {
				tokens = append(tokens, Token{Type: TokenAnd, Value: "and"})
				i += 3
				continue
			}
		}

		// 检查 OR 操作符（不区分大小写）
		if i+2 <= len(where) && strings.ToLower(where[i:i+2]) == "or" {
			// 确保 or 前后是空格或括号
			prevOk := i == 0 || where[i-1] == ' ' || where[i-1] == '\t' || where[i-1] == ')'
			nextOk := i+2 >= len(where) || where[i+2] == ' ' || where[i+2] == '\t' || where[i+2] == '('
			if prevOk && nextOk {
				tokens = append(tokens, Token{Type: TokenOr, Value: "or"})
				i += 2
				continue
			}
		}

		// 读取条件表达式（直到遇到 AND、OR 或括号）
		start := i
		for i < len(where) {
			if where[i] == '(' || where[i] == ')' {
				break
			}
			// 检查是否遇到 AND
			if i+3 <= len(where) && strings.ToLower(where[i:i+3]) == "and" {
				prevOk := where[i-1] == ' ' || where[i-1] == '\t'
				nextOk := i+3 >= len(where) || where[i+3] == ' ' || where[i+3] == '\t' || where[i+3] == '('
				if prevOk && nextOk {
					break
				}
			}
			// 检查是否遇到 OR
			if i+2 <= len(where) && strings.ToLower(where[i:i+2]) == "or" {
				prevOk := where[i-1] == ' ' || where[i-1] == '\t'
				nextOk := i+2 >= len(where) || where[i+2] == ' ' || where[i+2] == '\t' || where[i+2] == '('
				if prevOk && nextOk {
					break
				}
			}
			i++
		}

		condition := strings.TrimSpace(where[start:i])
		if condition != "" {
			tokens = append(tokens, Token{Type: TokenCondition, Value: condition})
		}
	}

	return tokens
}

// parseExpressionWithContext 安全的语法分析器：构建表达式树
func parseExpressionWithContext(tokens []Token, parseCtx *ParseContext) (*ExprNode, error) {
	// 检查递归深度
	parseCtx.depth++
	if parseCtx.depth > parseCtx.maxDepth {
		return nil, fmt.Errorf("recursion depth exceeded maximum: %d", parseCtx.maxDepth)
	}
	defer func() { parseCtx.depth-- }()

	// 检查上下文是否已取消
	select {
	case <-parseCtx.ctx.Done():
		return nil, parseCtx.ctx.Err()
	default:
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty tokens")
	}

	// 解析 OR 表达式（最低优先级）
	return parseOrExpressionWithContext(tokens, 0, parseCtx)
}

// parseExpression 语法分析器：构建表达式树（保留向后兼容性）
func parseExpression(tokens []Token) (*ExprNode, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty tokens")
	}

	// 解析 OR 表达式（最低优先级）
	return parseOrExpression(tokens, 0)
}

// parseOrExpressionWithContext 安全的 OR 表达式解析
func parseOrExpressionWithContext(tokens []Token, start int, parseCtx *ParseContext) (*ExprNode, error) {
	// 检查递归深度和上下文
	parseCtx.depth++
	if parseCtx.depth > parseCtx.maxDepth {
		return nil, fmt.Errorf("recursion depth exceeded maximum: %d", parseCtx.maxDepth)
	}
	defer func() { parseCtx.depth-- }()

	select {
	case <-parseCtx.ctx.Done():
		return nil, parseCtx.ctx.Err()
	default:
	}

	left, nextPos, err := parseAndExpressionWithContext(tokens, start, parseCtx)
	if err != nil {
		return nil, err
	}

	for nextPos < len(tokens) && tokens[nextPos].Type == TokenOr {
		// 边界检查
		if nextPos+1 >= len(tokens) {
			return nil, fmt.Errorf("incomplete OR expression at position %d", nextPos)
		}

		right, newPos, err := parseAndExpressionWithContext(tokens, nextPos+1, parseCtx)
		if err != nil {
			return nil, err
		}

		left = &ExprNode{
			Type:     NodeOperator,
			Operator: "or",
			Left:     left,
			Right:    right,
		}
		nextPos = newPos
	}

	return left, nil
}

// parseAndExpressionWithContext 安全的 AND 表达式解析
func parseAndExpressionWithContext(tokens []Token, start int, parseCtx *ParseContext) (*ExprNode, int, error) {
	// 检查递归深度和上下文
	parseCtx.depth++
	if parseCtx.depth > parseCtx.maxDepth {
		return nil, 0, fmt.Errorf("recursion depth exceeded maximum: %d", parseCtx.maxDepth)
	}
	defer func() { parseCtx.depth-- }()

	select {
	case <-parseCtx.ctx.Done():
		return nil, 0, parseCtx.ctx.Err()
	default:
	}

	left, nextPos, err := parsePrimaryExpressionWithContext(tokens, start, parseCtx)
	if err != nil {
		return nil, 0, err
	}

	for nextPos < len(tokens) && tokens[nextPos].Type == TokenAnd {
		// 边界检查
		if nextPos+1 >= len(tokens) {
			return nil, 0, fmt.Errorf("incomplete AND expression at position %d", nextPos)
		}

		right, newPos, err := parsePrimaryExpressionWithContext(tokens, nextPos+1, parseCtx)
		if err != nil {
			return nil, 0, err
		}

		left = &ExprNode{
			Type:     NodeOperator,
			Operator: "and",
			Left:     left,
			Right:    right,
		}
		nextPos = newPos
	}

	return left, nextPos, nil
}

// parsePrimaryExpressionWithContext 安全的基本表达式解析
func parsePrimaryExpressionWithContext(tokens []Token, start int, parseCtx *ParseContext) (*ExprNode, int, error) {
	// 检查递归深度和上下文
	parseCtx.depth++
	if parseCtx.depth > parseCtx.maxDepth {
		return nil, 0, fmt.Errorf("recursion depth exceeded maximum: %d", parseCtx.maxDepth)
	}
	defer func() { parseCtx.depth-- }()

	select {
	case <-parseCtx.ctx.Done():
		return nil, 0, parseCtx.ctx.Err()
	default:
	}

	// 边界检查
	if start >= len(tokens) {
		return nil, 0, fmt.Errorf("unexpected end of tokens at position %d", start)
	}

	token := tokens[start]

	// 处理括号表达式
	if token.Type == TokenLeftParen {
		// 查找匹配的右括号
		parenCount := 1
		end := start + 1
		for end < len(tokens) && parenCount > 0 {
			if end >= len(tokens) {
				return nil, 0, fmt.Errorf("unmatched left parenthesis at position %d", start)
			}
			if tokens[end].Type == TokenLeftParen {
				parenCount++
			} else if tokens[end].Type == TokenRightParen {
				parenCount--
			}
			if parenCount > 0 {
				end++
			}
		}

		if parenCount != 0 {
			return nil, 0, fmt.Errorf("unmatched parentheses starting at position %d", start)
		}

		// 递归解析括号内的表达式
		innerTokens := tokens[start+1 : end]
		node, err := parseExpressionWithContext(innerTokens, parseCtx)
		if err != nil {
			return nil, 0, err
		}

		return node, end + 1, nil
	}

	// 处理条件表达式
	if token.Type == TokenCondition {
		node := &ExprNode{
			Type:      NodeCondition,
			Condition: token.Value,
		}
		return node, start + 1, nil
	}

	return nil, 0, fmt.Errorf("unexpected token: %s at position %d", token.Value, start)
}

// parseOrExpression 解析 OR 表达式（保留向后兼容性）
func parseOrExpression(tokens []Token, start int) (*ExprNode, error) {
	left, nextPos, err := parseAndExpression(tokens, start)
	if err != nil {
		return nil, err
	}

	for nextPos < len(tokens) && tokens[nextPos].Type == TokenOr {
		right, newPos, err := parseAndExpression(tokens, nextPos+1)
		if err != nil {
			return nil, err
		}

		left = &ExprNode{
			Type:     NodeOperator,
			Operator: "or",
			Left:     left,
			Right:    right,
		}
		nextPos = newPos
	}

	return left, nil
}

// parseAndExpression 解析 AND 表达式
func parseAndExpression(tokens []Token, start int) (*ExprNode, int, error) {
	left, nextPos, err := parsePrimaryExpression(tokens, start)
	if err != nil {
		return nil, 0, err
	}

	for nextPos < len(tokens) && tokens[nextPos].Type == TokenAnd {
		right, newPos, err := parsePrimaryExpression(tokens, nextPos+1)
		if err != nil {
			return nil, 0, err
		}

		left = &ExprNode{
			Type:     NodeOperator,
			Operator: "and",
			Left:     left,
			Right:    right,
		}
		nextPos = newPos
	}

	return left, nextPos, nil
}

// parsePrimaryExpression 解析基本表达式（条件或括号表达式）
func parsePrimaryExpression(tokens []Token, start int) (*ExprNode, int, error) {
	if start >= len(tokens) {
		return nil, 0, fmt.Errorf("unexpected end of tokens")
	}

	token := tokens[start]

	// 处理括号表达式
	if token.Type == TokenLeftParen {
		// 查找匹配的右括号
		parenCount := 1
		end := start + 1
		for end < len(tokens) && parenCount > 0 {
			if tokens[end].Type == TokenLeftParen {
				parenCount++
			} else if tokens[end].Type == TokenRightParen {
				parenCount--
			}
			if parenCount > 0 {
				end++
			}
		}

		if parenCount != 0 {
			return nil, 0, fmt.Errorf("unmatched parentheses")
		}

		// 递归解析括号内的表达式
		innerTokens := tokens[start+1 : end]
		node, err := parseExpression(innerTokens)
		if err != nil {
			return nil, 0, err
		}

		return node, end + 1, nil
	}

	// 处理条件表达式
	if token.Type == TokenCondition {
		node := &ExprNode{
			Type:      NodeCondition,
			Condition: token.Value,
		}
		return node, start + 1, nil
	}

	return nil, 0, fmt.Errorf("unexpected token: %s", token.Value)
}

// evaluateExpressionWithContext 安全的递归求值表达式树（带短路优化）
func evaluateExpressionWithContext(src map[string]interface{}, node *ExprNode, parseCtx *ParseContext) (bool, error) {
	if node == nil {
		return true, nil
	}

	// 检查递归深度
	parseCtx.depth++
	if parseCtx.depth > parseCtx.maxDepth {
		// 超出递归深度，返回错误
		parseCtx.depth--
		return false, fmt.Errorf("recursion depth exceeded maximum: %d", parseCtx.maxDepth)
	}
	defer func() { parseCtx.depth-- }()

	// 检查上下文是否已取消
	select {
	case <-parseCtx.ctx.Done():
		return false, parseCtx.ctx.Err() // 超时或取消，返回具体错误
	default:
	}

	switch node.Type {
	case NodeCondition:
		// 叶子节点：调用原有的 whereFilter 处理单个条件
		return whereFilter(src, node.Condition), nil

	case NodeOperator:
		switch node.Operator {
		case "and":
			// AND短路求值：如果左边为false，直接返回false，不计算右边
			leftResult, err := evaluateExpressionWithContext(src, node.Left, parseCtx)
			if err != nil {
				return false, err
			}
			if !leftResult {
				return false, nil // 短路：AND操作左边为false，整个表达式为false
			}
			// 左边为true时才计算右边
			return evaluateExpressionWithContext(src, node.Right, parseCtx)

		case "or":
			// OR短路求值：如果左边为true，直接返回true，不计算右边
			leftResult, err := evaluateExpressionWithContext(src, node.Left, parseCtx)
			if err != nil {
				return false, err
			}
			if leftResult {
				return true, nil // 短路：OR操作左边为true，整个表达式为true
			}
			// 左边为false时才计算右边
			return evaluateExpressionWithContext(src, node.Right, parseCtx)

		default:
			return false, fmt.Errorf("unknown operator: %s", node.Operator)
		}

	default:
		return false, fmt.Errorf("unknown node type: %d", node.Type)
	}
}

// evaluateExpression 递归求值表达式树（保留向后兼容性）
func evaluateExpression(src map[string]interface{}, node *ExprNode) bool {
	if node == nil {
		return true
	}

	switch node.Type {
	case NodeCondition:
		// 叶子节点：调用原有的 whereFilter 处理单个条件
		return whereFilter(src, node.Condition)

	case NodeOperator:
		// 操作符节点：递归求值左右子树
		leftResult := evaluateExpression(src, node.Left)
		rightResult := evaluateExpression(src, node.Right)

		switch node.Operator {
		case "and":
			return leftResult && rightResult
		case "or":
			return leftResult || rightResult
		default:
			return false
		}

	default:
		return false
	}
}
