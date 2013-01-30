/*
	returns the number of rows contained by the given alias
*/
DEFINE row_count(data) returns count {
	grouped = GROUP $data ALL;
	$count = FOREACH grouped GENERATE COUNT_STAR($data);
};

/*
returns the number of rows for each key by the given alias
*/

DEFINE count_by(data, field) returns count {
	grouped = GROUP $data BY $field;
	$count = FOREACH grouped GENERATE group as $field, COUNT_STAR($data) as count;
};