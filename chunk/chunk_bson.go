package chunk

import "go.mongodb.org/mongo-driver/bson"

// ScanBson 生成 MongoDB 查询的 BSON 过滤器
// 参考 ScanWhereSQL 的逻辑，将 SQL WHERE 条件转换为 BSON 查询
func ScanBson(chunk *Chunk, scanRange string, next bool) (query bson.M) {
	query = toBsonQuery(chunk, next)

	// 如果有额外的扫描范围条件，需要与生成的查询合并
	if scanRange != "" && len(query) > 0 {
		// 将 scanRange 解析为 bson.M 并与现有查询合并
		// 这里假设 scanRange 是一个有效的 BSON 查询字符串或条件
		// 实际实现中可能需要更复杂的解析逻辑
		return bson.M{"$and": []bson.M{query, {"$expr": scanRange}}}
	} else if scanRange != "" {
		// 如果只有 scanRange 条件
		return bson.M{"$expr": scanRange}
	}
	return query
}

// toBsonQuery 将 Chunk 的边界条件转换为 BSON 查询
func toBsonQuery(chunk *Chunk, next bool) bson.M {
	// 处理边界情况：没有Bounds
	if len(chunk.Bounds) == 0 {
		return bson.M{}
	}

	// 单列情况下的简化处理
	if len(chunk.Bounds) == 1 {
		return BsonSimpleColumn(chunk, next)
	}

	// 多列情况下处理
	return bsonComplexColumn(chunk, next)
}

// bsonSimpleColumn 处理单列的 BSON 查询条件
func BsonSimpleColumn(chunk *Chunk, next bool) bson.M {
	bound := chunk.Bounds[0]
	column := bound.Column

	// 只有下界
	if bound.HasLower && !bound.HasUpper {
		if next {
			return bson.M{column: bson.M{"$gt": bound.Lower}}
		} else {
			return bson.M{column: bson.M{"$gte": bound.Lower}}
		}
	}

	// 只有上界
	if !bound.HasLower && bound.HasUpper {
		return bson.M{column: bson.M{"$lte": bound.Upper}}
	}

	// 同时有上下界且值相等
	if bound.HasLower && bound.HasUpper && bound.Lower == bound.Upper {
		return bson.M{column: bound.Lower}
	}

	// 同时有上下界且值不等
	if bound.HasLower && bound.HasUpper {
		rangeQuery := bson.M{}
		if next {
			rangeQuery["$gt"] = bound.Lower
		} else {
			rangeQuery["$gte"] = bound.Lower
		}
		rangeQuery["$lte"] = bound.Upper
		return bson.M{column: rangeQuery}
	}

	// 无上下界
	return bson.M{}
}

// bsonComplexColumn 处理多列的复合 BSON 查询条件
func bsonComplexColumn(chunk *Chunk, next bool) bson.M {
	var equalConditions bson.M = bson.M{}

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
		equalConditions[bound.Column] = bound.Lower
	}

	// 第二阶段：处理需要范围比较的列
	var lowerConditions []bson.M
	var upperConditions []bson.M

	for ; i < len(chunk.Bounds); i++ {
		bound := chunk.Bounds[i]
		isLastColumn := i == len(chunk.Bounds)-1

		// 构建前置相等条件（用于复合条件）
		preConditions := bson.M{}
		for key, value := range equalConditions {
			preConditions[key] = value
		}

		// 添加之前处理过的相等条件
		for j := 0; j < i; j++ {
			prevBound := chunk.Bounds[j]
			if prevBound.HasLower && prevBound.HasUpper && prevBound.Lower == prevBound.Upper {
				preConditions[prevBound.Column] = prevBound.Lower
			}
		}

		// 处理下界条件
		if bound.HasLower {
			lowerCond := bson.M{}
			// 复制前置条件
			for key, value := range preConditions {
				lowerCond[key] = value
			}

			// 添加当前列的下界条件
			if isLastColumn {
				if next {
					lowerCond[bound.Column] = bson.M{"$gt": bound.Lower}
				} else {
					lowerCond[bound.Column] = bson.M{"$gte": bound.Lower}
				}
			} else {
				lowerCond[bound.Column] = bson.M{"$gt": bound.Lower}
			}

			lowerConditions = append(lowerConditions, lowerCond)
		}

		// 处理上界条件
		if bound.HasUpper {
			upperCond := bson.M{}
			// 复制前置条件
			for key, value := range preConditions {
				upperCond[key] = value
			}

			// 添加当前列的上界条件
			if isLastColumn {
				upperCond[bound.Column] = bson.M{"$lte": bound.Upper}
			} else {
				upperCond[bound.Column] = bson.M{"$lt": bound.Upper}
			}

			upperConditions = append(upperConditions, upperCond)
		}
	}

	// 第三阶段：组合所有条件
	var finalConditions []bson.M

	// 添加相等条件
	if len(equalConditions) > 0 {
		finalConditions = append(finalConditions, equalConditions)
	}

	// 添加下界条件
	if len(lowerConditions) > 0 {
		if len(lowerConditions) == 1 {
			finalConditions = append(finalConditions, lowerConditions[0])
		} else {
			finalConditions = append(finalConditions, bson.M{"$or": lowerConditions})
		}
	}

	// 添加上界条件
	if len(upperConditions) > 0 {
		if len(upperConditions) == 1 {
			finalConditions = append(finalConditions, upperConditions[0])
		} else {
			finalConditions = append(finalConditions, bson.M{"$or": upperConditions})
		}
	}

	// 根据条件数量返回最终查询
	if len(finalConditions) == 0 {
		return bson.M{}
	} else if len(finalConditions) == 1 {
		return finalConditions[0]
	} else {
		return bson.M{"$and": finalConditions}
	}
}
