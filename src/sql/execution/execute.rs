use crate::common::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution::{aggregate, join, source, transform};
use crate::sql::planner::{BoxedNode, Node, Plan};
use crate::storage::page::RecordId;
use crate::storage::tuple::Rows;
use crate::types::field::{Field, Label};

/// Executes a query plan.
///
/// Takes both a catalog and transaction as parameters, even though a transaction
/// implements the Catalog trait, to separate the concerns of `catalog` to planning
/// and `txn` to execution.
///
/// Hint: `execute(source, txn)?` returns a `Rows` source iterator, which you might
/// need for some of the plans. (The `execute` method actually returns `Result<Rows>`,
/// but the `?` operator will automatically unwrap the result if it's an `Ok(Rows)`
/// value. Otherwise, the method will immediately exit and return the `Err()` value
/// returned from `execute`.) For more information about the try-operator `?`, see:
/// - https://doc.rust-lang.org/rust-by-example/std/result/question_mark.html
/// - https://stackoverflow.com/questions/42917566/what-is-this-question-mark-operator-about
pub fn execute_plan(
    plan: Plan,
    catalog: &impl Catalog,
    txn: &impl Transaction,
) -> Result<ExecutionResult> {
    Ok(match plan {
        // Creates a table with the given schema, returning a `CreateTable` execution
        // result if the table creation is successful.
        //
        // You'll need to handle the case when `Catalog::create_table` returns an Error
        // (hint: use the ? operator).
        Plan::CreateTable { schema } => {
            if let Err(e) = catalog.create_table(schema.clone()) {
                return Err(e);
            }
            ExecutionResult::CreateTable {
                name: schema.name().to_string(),
            }
        }
        // Deletes the rows emitted from the source node from the given table.
        //
        // Hint: you'll need to use the `write::delete` method that you also have implement,
        // which returns the number of rows that were deleted if successful (another hint:
        // use the ? operator. Last reminder!).
        Plan::Delete { table, source } => {
            // Execute the source node to get the rows to be deleted
            let rows = execute(source, txn)?;

            // Perform the delete operation using the write::delete function
            let deleted_count = match crate::sql::execution::write::delete(txn, table, rows) {
                Ok(count) => count,
                Err(e) => return Err(e),
            };

            // Return the result as ExecutionResult::Delete with the count of deleted rows
            ExecutionResult::Delete { count: deleted_count }
        }

        // Drops the given table.
        //
        // Returns an error if the table does not exist unless `if_exists` is true.
        Plan::DropTable { table, if_exists } => {
            // Attempt to drop the table using the catalog, handling existence based on `if_exists`
            let drop_result = catalog.drop_table(&table, if_exists);

            // Use explicit error handling instead of `?`
            let existed = match drop_result {
                Ok(existed) => existed,
                Err(e) => return Err(e),
            };

            // Construct the result for table drop
            ExecutionResult::DropTable {
                name: table,
                existed,
            }
        }
        // Inserts the rows emitted from the source node into the given table.
        //
        // Hint: you'll need to use the `write::insert` method that you have to implement,
        // which returns the record id's corresponding to the rows that were inserted into
        // the table.
        Plan::Insert { table, source } => {
            let rows = execute(source, txn)?;

            // Fetch the table schema using the catalog.
            let table_name = table.name(); // Extract the table name as &str
            let schema = catalog.get_table(&table_name)?.ok_or_else(|| {
                crate::common::Error::InvalidInput(format!("Table {} does not exist", table.name()))
            })?;

            // Use the `write::insert` function to insert the rows into the table.
            let record_ids = crate::sql::execution::write::insert(txn, schema, rows)?;

            // Return the number of rows inserted and their corresponding record IDs.
            let count = record_ids.len() as u64;
            ExecutionResult::Insert { count, record_ids }
        }
        // Obtains a `Rows` iterator of the emitted rows and the emitted rows' corresponding
        // column labels from the root node, packaging the two as an `ExecutionResult::Select`.
        //
        // Hint: the i'th column label of a row emitted from the root can be obtained by calling
        // `root.column_label(i)`.
        Plan::Select(root) => {
            let rows = execute(root.clone(), txn)?;

            // Collect column labels from the root node.
            let columns = (0..root.columns())
                .map(|i| root.column_label(i))
                .collect();

            // Return the result as an `ExecutionResult::Select`.
            ExecutionResult::Select { rows, columns }
        }
        // Updates the rows emitted from the source node in the given table.
        //
        // Hint: you'll have to use the `write::update` method that you have implement, which
        // returns the number of rows update if successful.
        Plan::Update {
            table,
            source,
            expressions,
        } => {
            let table_name = table.name();

            // Step 2: Execute the source node to obtain the rows to be updated.
            let rows = execute(source, txn)?;

            // Step 3: Fetch the schema of the table using `catalog.get_table`.
            let schema = catalog
                .get_table(table_name)?
                .ok_or_else(|| crate::common::Error::InvalidInput(format!("Table {} does not exist", table_name)))?;

            // Step 4: Use the `write::update` method to perform the update operation.
            let updated_count = crate::sql::execution::write::update(txn, table_name.to_string(), rows, expressions)?;

            // Step 5: Return an `ExecutionResult::Update` with the count of updated rows.
            ExecutionResult::Update { count: updated_count }
        }
    })
}

/// Recursively executes a query plan node, returning a tuple iterator.
///
/// Tuples stream through the plan node tree from the branches to the root. Nodes
/// recursively pull input rows upwards from their child node(s), process them,
/// and hand the resulting rows off to their parent node.
pub fn execute(node: BoxedNode, txn: &impl Transaction) -> Result<Rows> {
    Ok(match *node.inner {
        Node::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let source = execute(source, txn)?;
            aggregate::aggregate(source, group_by, aggregates)?
        }

        Node::Filter { source, predicate } => {
            // Execute the source node to get the input rows.
            let source_rows = execute(source, txn)?;

            // Define the filter operation using `filter_map`.
            let filtered_rows = source_rows.filter_map(move |result| {
                result
                    .and_then(|(record_id, row)| {
                        // Evaluate the predicate expression.
                        match predicate.evaluate(Some(&row))? {
                            // Keep the row if the predicate evaluates to `true`.
                            Field::Boolean(true) => Ok(Some((record_id, row))),
                            // Skip the row if the predicate evaluates to `false` or `NULL`.
                            Field::Boolean(false) | Field::Null => Ok(None),
                            // Return an error if the predicate does not return a boolean value.
                            value => Err(crate::common::Error::InvalidInput(format!(
                                "Filter predicate returned {value}, expected boolean."
                            ))),
                        }
                    })
                    .transpose() // Convert Option<Result<T>> to Result<Option<T>>
            });

            // Wrap the filtered rows in `Ok` to match the expected return type.
            Box::new(filtered_rows)
        }

        Node::HashJoin {
            left,
            left_column,
            right,
            right_column,
            outer,
        } => {
            let right_size = right.columns();
            let left = execute(left, txn)?;
            let right = execute(right, txn)?;
            join::hash(left, left_column, right, right_column, right_size, outer)?
        }

        Node::IndexLookup {
            table: _table,
            column: _column,
            values: _values,
            alias: _,
        } => {
            todo!();
        }

        Node::KeyLookup {
            table: _table,
            keys: _keys,
            alias: _,
        } => {
            todo!();
        }

        Node::Limit { source, limit } => {
            let source_rows = execute(source, txn)?;

            // Apply the `limit` transformation to restrict the number of rows.
            let limited_rows = crate::sql::execution::transform::limit(source_rows, limit);

            // Return the limited rows.
            limited_rows
        }

        Node::NestedLoopJoin {
            left,
            right,
            predicate,
            outer,
        } => {
            let right_size = right.columns();
            let left = execute(left, txn)?;
            let right = execute(right, txn)?;
            join::nested_loop(left, right, right_size, predicate, outer)?
        }

        Node::Nothing { .. } => source::nothing(),

        Node::Offset {
            source: _source,
            offset: _offset,
        } => {
            todo!();
        }

        Node::Order {
            source,
            key: orders,
        } => {
            let source = execute(source, txn)?;
            transform::order(source, orders)?
        }

        Node::Projection {
            source,
            expressions,
            aliases: _,
        } => {
            let source_rows = execute(source, txn)?;

            // Apply the `project` method to transform the input rows using the provided expressions.
            let projected_rows = crate::sql::execution::transform::project(source_rows, expressions);

            // Return the projected rows as the result.
            projected_rows
        }

        Node::Remap { source, targets } => {
            let source = execute(source, txn)?;
            transform::remap(source, targets)
        }

        Node::Scan {
            table,
            filter,
            alias: _,
        } => {
            let table_name = table.name(); // Ensure this returns a &str
            let rows = txn.scan(table_name, filter)?; // Unwrap the result using `?`
            rows // Directly return the `Rows` type
        }

        Node::Values { rows } => source::values(rows),
    })
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable {
        name: String,
    },
    DropTable {
        name: String,
        existed: bool,
    },
    Delete {
        count: u64,
    },
    Insert {
        count: u64,
        record_ids: Vec<RecordId>,
    },
    Update {
        count: u64,
    },
    Select {
        rows: Rows,
        columns: Vec<Label>,
    },
}
