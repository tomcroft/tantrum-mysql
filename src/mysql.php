<?php

use tantrum\QueryBuilder;

class tantrum_mysql_adaptor
{
	protected static $schema;
	protected static $nonEscapedStrings = array('NOW()', null);
	
	public static function formatSelect(QueryBuilder\Query $query)
	{
		$fields = !$query->GetFields()->isEmpty()?implode(','.PHP_EOL, array_keys($query->getFields()->toArray())):'*';
		$queryString = 'SELECT '.PHP_EOL.$fields.PHP_EOL.' FROM '.PHP_EOL.$query->getTarget();
		$queryString .= $query->GetAlias()?' AS '.$query->getAlias().PHP_EOL:''.PHP_EOL;

		foreach($query->getJoins() as $join) {
			$queryString .= self::formatJoin($join);
		}
		$clauses = $query->getClauses();

		if(count($clauses) > 0) {
			$queryString .= ' WHERE ';
			
			foreach($clauses as $clause) {
				if($clause instanceof QueryBuilder\Clause) {
					$queryString .= self::formatClause($clause);
				} elseif($clause instanceof QueryBuilder\ClauseCollection) {
					$queryString .= self::formatClauseCollection($clause);
				}
			}
		}
		$queryString .= self::formatGroupBy($query->getGroupBy());
		$queryString .= self::formatOrderBy($query->getOrderBy());
		$queryString .= self::formatLimit($query->getOffset(), $query->getLimit());
		
		return $queryString;
	}
    
	public static function formatInsert(QueryBuilder\Query $query)
	{
		$placeholders = array_fill(0, count($query->getFields()->ToArray()), '?');
		$queryString = 'INSERT INTO '.$query->getTarget().
			' ('.implode(',',array_keys($query->GetFields()->ToArray())).')'.
			' VALUES '.
			' ('.implode(',', $placeholders).')';
		if(!is_null($query->getDuplicateFieldsForUpdate())) {
			$queryString .= ' ON DUPLICATE KEY UPDATE ';
			$fields = array();
			foreach(array_keys($query->getDuplicateFieldsForUpdate()->toArray()) as $key) {
				$fields[] = $key.' = ?';
			}
		$queryString .= implode(',',$fields);
		}
		return $queryString;
	}

	public static function formatDelete(QueryBuilder\Query $query)
	{
		$queryString = 'DELETE FROM '.$query->getTarget();
		$queryString .= $query->GetAlias()?' AS '.$query->getAlias().PHP_EOL:''.PHP_EOL;
		foreach($query->getJoins() as $join) {
			$queryString .= self::formatJoin($join);
		}
		$queryString .= ' WHERE ';
		foreach($query->getClauses() as $clause) {
			if($clause instanceof QueryBuilder\Clause) {
				$queryString .= self::formatClause($clause);
			} elseif($clause instanceof QueryBuilder\ClauseCollection) {
				$queryString .= $this->formatClauseCollection($clause);
			}
		}
		$queryString .= $this->formatGroupBy($query->getGroupBy());
		$queryString .= $this->formatOrderBy($query->getOrderBy());
		$queryString .= $this->formatLimit($query->getOffset(), $query->getLimit()); 
		return $queryString;
	}
   
	public static function formatUpdate(QueryBuilder\Query $query)
	{
		$queryString = 'UPDATE '.$query->getTarget();
		
		$queryString .= $query->getAlias()?' AS '.$query->getAlias().' SET '.PHP_EOL:' SET '.PHP_EOL;
		
		$queryString .= implode(' = ?, ', array_keys($query->getFields()->toArray())).' = ?';
		
		$queryString .= ' WHERE ';
		foreach($query->getClauses() as $clause) {
			if($clause instanceof QueryBuilder\Clause) {
				$queryString .= self::formatClause($clause);
			} elseif($clause instanceof QueryBuilder\ClauseCollection) {
				$queryString .= self::formatClauseCollection($clause);
			}
		}
		return $queryString;
	}
	
	public static function getColumnDefinitions($schema, $table)
	{
		$query = QueryBuilder\Query::Select('information_schema.COLUMNS','c',
			new QueryBuilder\Fields('c.COLUMN_NAME AS columnName',
				'c.DATA_TYPE AS dataType',
				'IF(c.IS_NULLABLE="No",1,0) AS required',
				'c.CHARACTER_MAXIMUM_LENGTH AS maximumLength',
				'c.COLUMN_KEY AS columnKey',
				'kcu.REFERENCED_TABLE_SCHEMA as joinDatabase',
				'kcu.REFERENCED_TABLE_NAME as joinTable',
				'kcu.REFERENCED_COLUMN_NAME as joinOn',
				'0 as modified',
				'IF(kcu2.COLUMN_NAME IS NOT NULL, 1, 0) AS hasExternalReferences',
				'c.ORDINAL_POSITION AS ordinalPosition',
				'kcu.POSITION_IN_UNIQUE_CONSTRAINT AS positionInUniqueConstraint'))
			->LeftJoin('information_schema.KEY_COLUMN_USAGE', QueryBuilder\Clause::On('kcu.COLUMN_NAME','c.COLUMN_NAME'), 'kcu')
			->LeftJoin('information_schema.KEY_COLUMN_USAGE', QueryBuilder\Clause::On('kcu2.TABLE_SCHEMA','c.TABLE_NAME')->And('kcu2.COLUMN_NAME', 'c.COLUMN_NAME', QueryBuilder\Clause::EQUALS, false), 'kcu2')
			->Where('c.TABLE_SCHEMA', $schema)
			->And('c.TABLE_NAME', $table)
			->GroupBy('concat(c.COLUMN_NAME, c.TABLE_NAME, c.TABLE_SCHEMA)')
			->OrderBy('c.ORDINAL_POSITION');

		return $query;
		//$this->Query($query);
		//$fields = $this->FetchAll('tantrum\QueryBuilder\Field');

		/*foreach($arrDBColumns as $arrColumnDefinition)
		{
			$arrColumns[self::MapColumnName($arrColumnDefinition['strColumnName'])] = array (
				'bolRequired' => $arrColumnDefinition['strColumnKey']!='PRI'?$arrColumnDefinition['bolRequired']:NULL,
				'strDataType' => $arrColumnDefinition['strDataType'],
				'intMaximumLength' => $arrColumnDefinition['intMaximumLength'],
				'strColumnKey' => $arrColumnDefinition['strColumnKey'],
				'strJoinDatabase' => $arrColumnDefinition['strJoinDatabase'],
				'strJoinTable' => $arrColumnDefinition['strJoinTable'],
				'strJoinOn' => $arrColumnDefinition['strJoinOn'],
				'bolExtentionColumn' => $arrColumnDefinition['bolExtensionColumn']
			);
			
			if($arrColumnDefinition['bolHasExternalReferences'] == 1)
			{
				// Remind me again, what are external references for???
				// maybe they were for determining whether a column was editable...
				// maybe to work out if this is a link / lookup table.
				$arrExternalReferences = $this->GetExternalReferences($this->strSchema, $strTable, $arrColumnDefinition['strColumnName']);
			}
		} */
		//TODO: Determine the relationship
		return $fields;
	}
	
	protected function getExternalReferences($schema, $table, $column)
	{
		$queryString = '
			SELECT
				REFERENCED_COLUMN_NAME AS strColumnName,
				REFERENCED_TABLE_NAME AS strTableName,
				REFERENCED_TABLE_SCHEMA as strDatabaseName
			FROM
				information_schema.KEY_COLUMN_USAGE
			WHERE
				TABLE_SCHEMA = '.$this->Escape($schema).'
			AND
				TABLE_NAME = '.$this->Escape($table).'
			AND
				COLUMN_NAME = '.$this->Escape($column);

		$this->Query(queryString);
		return $this->FetchAll();
	}
	
	protected static function formatJoin(QueryBuilder\Join $join)
	{
		switch ($join->getType()) {
			case QueryBuilder\Join::INNER:
				$joinType = 'INNER';
			break;
			case QueryBuilder\Join::LEFT:
				$joinType = 'LEFT';
			break;
			default:
				throw new Exception\DatabaseDException('Join type not handled');
			break;
		}
		return sprintf(' %s JOIN %s AS %s %s', $joinType, $join->getTarget(), $join->getAlias(), self::formatClauseCollection($join->getClauseCollection())).PHP_EOL;
	}
	
	protected static function formatClause(QueryBuilder\Clause $clause, $clauseString = '')
	{
		list($left, $right) = $clause->getArgs();
		
		$clauseString .= self::formatOperator($clause->getType());
		$clauseString .= $left;
		$clauseString .= self::formatOperator($clause->getOperator());
		$clauseString .= $clause->isEscaped()?'?':$right;
	
		return $clauseString."\r\n";
	}
	
	protected static function formatClauseCollection(QueryBuilder\ClauseCollection $clauseCollection)
	{
		switch($clauseCollection->count()) {
			case 0: 
				return '';
				break;
			case 1:
				return self::formatClause($clauseCollection->toArray()[0]);
				break;
			default:
				$return = ($clauseCollection->getType()==QueryBuilder\Clause::ON)?'':self::formatOperator($clauseCollection->getType()).'(';
				foreach($clauseCollection->toArray() as $clause) {
					$return = self::formatClause($clause, $return);
				}
				$return .= ($clauseCollection->getType()==QueryBuilder\Clause::ON)?'':')';
				return $return;
				break;
		}
	}
	
	protected static function formatOperator($operator)
	{
		switch($operator) {
			case QueryBuilder\Clause::WHERE:
				return '';
				break;
			case QueryBuilder\Clause::EQUALS:
				return ' = ';
				break;
			case QueryBuilder\Clause::NOT_EQUAL:
				return ' <=> ';
				break;
			case QueryBuilder\Clause::LESS_THAN:
				return ' < ';
				break;
			case QueryBuilder\Clause::GREATER_THAN:
				return ' > ';
				break;
			case QueryBuilder\Clause::_AND:
				return ' AND ';
				break;
			case QueryBuilder\Clause::_OR:
				return ' OR ';
				break;
			case QueryBuilder\Clause::ON:
				return ' ON ';
				break;
			default:
				throw new Exception\DatabaseException('Operator not handled');
				break;
		}
	}
	
	protected static function formatGroupBy($groupBy)
	{
		if(count($groupBy) > 0) {
			return ' GROUP BY '.implode(PHP_EOL, $groupBy);
		}
	}
	
	protected static function formatOrderBy($orderBy)
	{
		if(count($orderBy) == 0) {
			return;
		}
		$orderString = PHP_EOL.' ORDER BY ';
		foreach($orderBy as $field => $direction) {
			$orderString .= $field;
			switch($direction) {
			 	case QueryBuilder\Query::ASC:
			 		$orderString .= ' ASC';
			 		break;
			 	case QueryBuilder\Query::DESC:
			 		$orderString .= ' DESC';
			 		break;
			 	default:
			 		throw new Exception\Exception('Order by direction not handled');
			 		break;
			}
		}
		return $orderString;
	}
	
	protected static function formatLimit($number, $offset)
	{
		if(is_null($number) && is_null($offset)) {
			return;
		} elseif($number > 0 && is_null($offset)) {
			return sprintf(' LIMIT %u', $number);
		} else {
			return sprintf(' LIMIT %u,%u ', $number, $offset);
		}
	}
	
	protected static function formatFields($fields)
	{
		$return = '';

		foreach($fields->toArray() as $key => $value) {
			$return .= ' '.$key.' = ?';
		}
		return $return;
	}
}
