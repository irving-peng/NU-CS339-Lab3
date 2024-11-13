use crate::common::Result;
use crate::sql::engine::Transaction;
use crate::sql::planner::Expression;
use crate::storage::page::RecordId;
use crate::storage::tuple::Rows;
use crate::types::Table;

/// Deletes rows, taking primary keys from the source (i.e. DELETE) using the
/// primary_key column index. Returns the number of rows deleted.
pub fn delete(txn: &impl Transaction, table: String, source: Rows) -> Result<u64> {
    let mut count = 0;

    for result in source {
        let (record_id, _) = result?; // Unwrap the Result to get (RecordId, Row)
        txn.delete(&table, &[record_id])?;
        count += 1;
    }

    Ok(count)
}

/// Inserts rows into a table (i.e. INSERT) from the given source.
/// Returns the record IDs corresponding to the rows inserted into the table.
pub fn insert(txn: &impl Transaction, table: Table, source: Rows) -> Result<Vec<RecordId>> {
    let mut record_ids = Vec::new();

    // Store the table name to avoid multiple calls and moving issues
    let table_name = table.name().clone();

    // Insert each row into the table
    for result in source {
        let (_, row) = result?; // Unwrap each row from the Result
        let tuple = row.to_tuple(&table)?; // Convert row to tuple based on schema

        // Insert the tuple into the transaction and retrieve the record IDs
        let inserted_ids = txn.insert(&table_name, vec![row])?; // Directly pass `row`

        // Add the first record ID to the list of record_ids
        record_ids.push(inserted_ids[0].clone());
    }

    Ok(record_ids)
}

/// Updates rows passed in from the source (i.e. UPDATE). Returns the number of
/// rows updated.
///
/// Hint: `<T,E> Option<Result<T,E>>::transpose(self) -> Result<Option<T>, E>` and
/// the `?` operator might be useful here. An example of `transpose` from the docs:
/// ```
/// #[derive(Debug, Eq, PartialEq)]
/// struct SomeErr;
///
/// let x: Result<Option<i32>, SomeErr> = Ok(Some(5));
/// let y: Option<Result<i32, SomeErr>> = Some(Ok(5));
/// assert_eq!(x, y.transpose());
/// ```
pub fn update(
    txn: &impl Transaction,
    table: String,
    mut source: Rows,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {
    let mut count = 0;

    for result in source {
        let (record_id, mut row) = result?;

        // Apply each expression to the specified column index
        for (index, expr) in &expressions {
            let value = expr.evaluate(Some(&row))?;
            row.update_field(*index, value)?; // Use `update_field` to modify the field
        }

        // Update the row in the transaction
        txn.update(&table, [(record_id, row)].iter().cloned().collect())?;
        count += 1; // Increment the count of updated rows
    }

    Ok(count) // Return the total count of updated rows
}
