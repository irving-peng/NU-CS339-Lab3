use crate::common::Result;
use crate::sql::planner::Direction;
use crate::sql::planner::Expression;
use crate::storage::tuple::{Row, Rows};
use crate::types::field::Field;
use itertools::{izip, Itertools as _};

/// Filters the input rows (i.e. WHERE).
///
/// (Hint: look at the `iterator.rs` standard library API. There's a
/// method that returns an iterator that only emits elements that
/// satisfy a given predicate.)
pub fn filter(source: Rows, predicate: Expression) -> Rows {
    // Use `filter_map` to evaluate the predicate and filter rows.
    Box::new(source.filter_map(move |result| {
        match result {
            Ok((rid, row)) => {
                // Evaluate the predicate expression on the current row.
                match predicate.evaluate(Some(&row)) {
                    Ok(Field::Boolean(true)) => Some(Ok((rid, row))), // Include the row if predicate is true.
                    Ok(Field::Boolean(false)) | Ok(Field::Null) => None, // Exclude the row if predicate is false or null.
                    Ok(value) => Some(Err(crate::common::Error::InvalidInput(format!(
                        "Filter predicate returned {value}, expected a boolean."
                    )))),
                    Err(e) => Some(Err(e)),
                }
            }
            Err(e) => Some(Err(e)),
        }
    }))
}

/// Limits the result to the given number of rows (i.e. LIMIT).
///
/// (Hint: look at the `iterator.rs` standard library API. There's a
/// method that limits the iterator to a specified number of elements.)
pub fn limit(source: Rows, limit: usize) -> Rows {
    Box::new(source.take(limit))
}

/// Skips the given number of rows (i.e. OFFSET).
#[allow(dead_code)]
pub fn offset(source: Rows, offset: usize) -> Rows {
    Box::new(source.skip(offset))
}

/// Sorts the rows (i.e. ORDER BY).
pub fn order(source: Rows, order: Vec<(Expression, Direction)>) -> Result<Rows> {
    // We can't use sort_by_cached_key(), since expression evaluation is
    // fallible, and since we may have to vary the sort direction of each
    // expression. Precompute the sort values instead, and map them based on
    // the row index.
    let mut irows: Vec<_> = source
        .enumerate()
        .map(|(i, r)| r.map(|row| (i, row)))
        .try_collect()?;
    let mut sort_values = Vec::with_capacity(irows.len());
    for (_, (_rid, row)) in &irows {
        let values: Vec<_> = order
            .iter()
            .map(|(e, _)| e.evaluate(Some(&row)))
            .try_collect()?;
        sort_values.push(values)
    }

    irows.sort_by(|&(a, _), &(b, _)| {
        let dirs = order.iter().map(|(_, dir)| dir);
        for (a, b, dir) in izip!(&sort_values[a], &sort_values[b], dirs) {
            match a.cmp(b) {
                std::cmp::Ordering::Equal => {}
                order if *dir == Direction::Descending => return order.reverse(),
                order => return order,
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(Box::new(irows.into_iter().map(|(_, row)| Ok(row))))
}

/// Projects the rows using the given expressions (i.e. SELECT).
///
/// (Hint: The result of calling Expression::evaluate(row: Option<&Row>)
/// to evaluate the expression on a given row.)
/// (Hint 2: Each expression in expressions corresponds to a column that
/// the projection is selecting for. You'll want to build a projection
/// row from the results of calling each expression on a given row.)
pub fn project(source: Rows, expressions: Vec<Expression>) -> Rows {
    let projected_rows = source.map(move |result| {
        result.and_then(|(record_id, row)| {
            // Evaluate each expression and collect the results.
            let projected_fields: Result<Vec<Field>> = expressions
                .iter()
                .map(|expr| expr.evaluate(Some(&row)))
                .collect();

            // Create a new projected row from the evaluated fields.
            let new_row = Row::from(projected_fields?);

            // Return the transformed (RecordId, Row) pair.
            Ok((record_id, new_row))
        })
    });

    // Wrap the projected rows in a `Box` to return as `Rows`.
    Box::new(projected_rows)
}

/// Remaps source columns to target column indexes, or drops them if None.
pub fn remap(source: Rows, targets: Vec<Option<usize>>) -> Rows {
    let size = targets
        .iter()
        .filter_map(|v| *v)
        .map(|i| i + 1)
        .max()
        .unwrap_or(0);
    Box::new(source.map_ok(move |(rid, row)| {
        let mut out = vec![Field::Null; size];
        for (value, target) in row.into_iter().zip(&targets) {
            if let Some(index) = target {
                out[*index] = value;
            }
        }
        (rid, Row::from(out))
    }))
}
