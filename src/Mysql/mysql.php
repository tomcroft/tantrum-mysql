<?php

namespace tantrum\mysql;

use tomcroft\tantrum\Querybuilder;

class mysql
{
	protected $schema;
	protected $nonEscapedStrings = array('NOW()', null);

 	public function __construct($schema = 'information_schema')
	{
		parent::__construct('mysql', $schema);
	}
	
	public function FormatSelect(QueryBuilder\Query $query)
	{
		$fields = !$query->GetFields()->IsEmpty()?implode(','.PHP_EOL, array_keys($query->GetFields()->ToArray())):'*';
		$queryString = 'SELECT '.PHP_EOL.$fields.PHP_EOL.' FROM '.PHP_EOL.$query->GetFrom();
		$queryString .= $query->GetAlias()?' AS '.$query->GetAlias().PHP_EOL:''.PHP_EOL;

		foreach($query->GetJoins() as $join) {
			$queryString .= $this->FormatJoin($join);
		}
		$clauses = $query->GetClauses();

		if(count($clauses) > 0) {
			$queryString .= ' WHERE ';
			
			foreach($clauses as $clause) {
				if($clause instanceof QueryBuilder\Clause) {
					$queryString .= $this->FormatClause($clause);
				} elseif($clause instanceof QueryBuilder\ClauseCollection) {
					$queryString .= $this->FormatClauseCollection($clause);
				}
			}
		}
		$queryString .= $this->FormatGroupBy($query->GetGroupBy());
		$queryString .= $this->FormatOrderBy($query->GetOrderBy());
		$queryString .= $this->FormatLimit($query->GetStart(), $query->GetOffset());
		
		return $queryString;
	}
    
  public function FormatInsert(QueryBuilder\Query $query)
  {
  	$placeholders = array_fill(0, count($query->GetFields()->ToArray()), '?');
  	$queryString = 'INSERT INTO '.$query->GetFrom().
  		' ('.implode(',',array_keys($query->GetFields()->ToArray())).')'.
  		' VALUES '.
  		' ('.implode(',', $placeholders).')';
  	if(!is_null($query->GetDuplicateFieldsForUpdate())) {
  		$queryString .= ' ON DUPLICATE KEY UPDATE ';
  		$fields = array();
  		foreach(array_keys($query->GetDuplicateFieldsForUpdate()->ToArray()) as $key) {
  			$fields[] = $key.' = ?';
		}
		$queryString .= implode(',',$fields);
  	}
  	return $queryString;
  }
  
  public function FormatDelete(QueryBuilder\Query $query)
  {
  	$queryString = 'DELETE FROM '.$query->GetFrom();
  	$queryString .= $query->GetAlias()?' AS '.$query->GetAlias().PHP_EOL:''.PHP_EOL;
  	foreach($query->GetJoins() as $join) {
		$queryString .= $this->FormatJoin($join);
	}
	$queryString .= ' WHERE ';
  	foreach($query->GetClauses() as $clause) {
  		if($clause instanceof QueryBuilder\Clause) {
  			$queryString .= $this->FormatClause($clause);
  		} elseif($clause instanceof QueryBuilder\ClauseCollection) {
  			$queryString .= $this->FormatClauseCollection($clause);
  		}
  	}
  	$queryString .= $this->FormatGroupBy($query->GetGroupBy());
	$queryString .= $this->FormatOrderBy($query->GetOrderBy());
	$queryString .= $this->FormatLimit($query->GetStart(), $query->GetOffset()); 
  	return $queryString;
  }
   
  public function FormatUpdate(QueryBuilder\Query $query)
  {
  	$queryString = 'UPDATE '.$query->GetFrom();
  	
  	$queryString .= $query->GetAlias()?' AS '.$query->GetAlias().' SET '.PHP_EOL:' SET '.PHP_EOL;
  	
  	$queryString .= implode(' = ?, ', array_keys($query->GetFields()->ToArray())).' = ?';
  	
  	$queryString .= ' WHERE ';
  	foreach($query->GetClauses() as $clause) {
  		if($clause instanceof QueryBuilder\Clause) {
  			$queryString .= $this->FormatClause($clause);
  		} elseif($clause instanceof QueryBuilder\ClauseCollection) {
  			$queryString .= $this->FormatClauseCollection($clause);
  		}
  	}
  	return $queryString;
  }
	
	public function getColumnDefinitions($table)
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
			->Where('c.TABLE_SCHEMA', $this->schema)
			->And('c.TABLE_NAME', $table)
			->GroupBy('concat(c.COLUMN_NAME, c.TABLE_NAME, c.TABLE_SCHEMA)')
			->OrderBy('c.ORDINAL_POSITION');

		$this->Query($query);
		$fields = $this->FetchAll('tantrum\QueryBuilder\Field');

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
	
	protected function GetExternalReferences($database, $table, $column)
	{
		$queryString = '
			SELECT
				REFERENCED_COLUMN_NAME AS strColumnName,
				REFERENCED_TABLE_NAME AS strTableName,
				REFERENCED_TABLE_SCHEMA as strDatabaseName
			FROM
				information_schema.KEY_COLUMN_USAGE
			WHERE
				TABLE_SCHEMA = '.$this->Escape($database).'
			AND
				TABLE_NAME = '.$this->Escape($table).'
			AND
				COLUMN_NAME = '.$this->Escape($column);

		$this->Query(queryString);
		return $this->FetchAll();
	}
	
	protected function FormatJoin(QueryBuilder\Join $join)
	{
		switch ($join->GetType()) {
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
		return sprintf(' %s JOIN %s AS %s %s', $joinType, $join->GetTarget(), $join->GetAlias(), $this->FormatClauseCollection($join->GetClauseCollection())).PHP_EOL;
	}
	
	protected function FormatClause(QueryBuilder\Clause $clause, $clauseString = '')
	{
		list($left, $right) = $clause->getArgs();
		
		$clauseString .= $this->FormatOperator($clause->getType());
		$clauseString .= $left;
		$clauseString .= $this->FormatOperator($clause->getOperator());
		$clauseString .= $clause->Escape()?'?':$right;
	
		return $clauseString."\r\n";
	}
	
	protected function FormatClauseCollection(QueryBuilder\ClauseCollection $clauseCollection)
	{
		switch($clauseCollection->Count()) {
			case 0: 
				return '';
				break;
			case 1:
				return $this->FormatClause($clauseCollection->ToArray()[0]);
				break;
			default:
				$strReturn = ($clauseCollection->GetType()==QueryBuilder\Clause::ON)?'':$this->FormatOperator($clauseCollection->GetType()).'(';
				foreach($clauseCollection->ToArray() as $clause) {
					$return = $this->FormatClause($clause, $return);
				}
				$return .= ($clauseCollection->GetType()==QueryBuilder\Clause::ON)?'':')';
				return $return;
				break;
		}
	}
	
	protected function FormatOperator($operator)
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
	
	protected function FormatGroupBy($groupBy)
	{
		if(count($groupBy) > 0) {
			return ' GROUP BY '.implode("\r\n",$groupBy);
		}
	}
	
	protected function FormatOrderBy($orderBy)
	{
		if(count($orderBy) == 0) {
			return;
		}
		$orderString = PHP_EOL.' ORDER BY ';
		foreach($orderBy as $field => $direction) {
			$orderString .= $field;
			switch($direction) {
			 	case Query::ASC:
			 		$orderString .= ' ASC';
			 		break;
			 	case Query::DESC:
			 		$orderString .= ' DESC';
			 		break;
			 	default:
			 		throw new Exception\Exception('Order by direction not handled');
			 		break;
			}
		}
		return $orderString;
	}
	
	protected function FormatLimit($number, $offset)
	{
		if(is_null($number) && is_null($offset)) {
			return;
		} elseif($number > 0 && is_null($offset)) {
			return sprintf(' LIMIT %u', $number);
		} else {
			return sprintf(' LIMIT %u,%u ', $number, $offset);
		}
	}
	
	protected function FormatFields($fields)
	{
		$return = '';

		foreach($fields->ToArray() as $key => $value) {
			$return .= ' '.$key.' = ?';
		}
		return $return;
	}
}
