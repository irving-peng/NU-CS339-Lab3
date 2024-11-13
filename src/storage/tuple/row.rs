use crate::common::{Error, Result};
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use crate::types::field::Field;
use crate::types::{DataType, Table};
use dyn_clone::DynClone;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::slice::Iter;

/// A row iterator.
pub type Rows = Box<dyn RowIterator>;

/// A Row iterator trait, which requires the iterator to be both clonable and
/// object-safe. Cloning is needed to be able to reset an iterator back to an
/// initial state, e.g. during nested loop joins. It has a blanket
/// implementation for all matching iterators.
pub trait RowIterator: Iterator<Item = Result<(RecordId, Row)>> + DynClone {}
impl<I: Iterator<Item = Result<(RecordId, Row)>> + DynClone> RowIterator for I {}
dyn_clone::clone_trait_object!(RowIterator);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Row {
    values: Vec<Field>,
}

impl From<Vec<Field>> for Row {
    fn from(v: Vec<Field>) -> Self {
        Row::new(v)
    }
}

impl From<Vec<&Field>> for Row {
    fn from(value: Vec<&Field>) -> Self {
        Row::new(value.into_iter().cloned().collect())
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.values.eq(&other.values)
    }
    fn ne(&self, other: &Self) -> bool {
        self.values.ne(&other.values)
    }
}

impl IntoIterator for Row {
    type Item = Field;
    type IntoIter = std::vec::IntoIter<Field>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl Row {
    fn new(values: Vec<Field>) -> Row {
        Row {
            values: values.to_vec(),
        }
    }

    pub fn iter(&self) -> Iter<Field> {
        self.values.iter()
    }

    pub fn size(&self) -> usize {
        self.values.len()
    }

    pub fn get_field(&self, index: usize) -> Result<Field> {
        Ok(self
            .values
            .get(index)
            .ok_or_else(|| Error::OutOfBounds)?
            .clone())
    }

    pub fn update_field(&mut self, index: usize, new: Field) -> Result<()> {
        let field = self
            .values
            .get_mut(index)
            .ok_or_else(|| Error::OutOfBounds)?;

        match field.get_type() == new.get_type() {
            true => {
                *field = new;
                Ok(())
            }
            false => Result::from(Error::InvalidInput(new.to_string())),
        }
    }

    pub fn to_string(&self, str_len: Option<usize>) -> String {
        self.values
            .iter()
            .map(|field| match field.get_type() {
                DataType::Text => {
                    let mut text = field.to_string();
                    if let Some(len) = str_len {
                        text.truncate(len);
                    }
                    text
                }
                _ => field.to_string(),
            })
            .join(", ")
    }

    pub fn to_tuple(&self, schema: &Table) -> Result<Tuple> {
        Ok(Tuple::from(self.serialize(schema)?))
    }

    pub fn from_tuple(tuple: Tuple, schema: &Table) -> Result<Row> {
        Ok(Self::deserialize(tuple.data, schema))
    }

    /// Serializes the Row's header and data into a byte-stream, structured as follows:
    ///
    /// | variable length field offset map | field data in bytes |
    ///                 ^                               ^
    ///     a text field's `stored_offset` points       |
    ///     here, which stores the field's offset into here
    ///
    ///   a fixed length field's stored_offset is to the offset from the start of
    ///   the field data portion (possibly not the beginning of the byte stream!)
    pub fn serialize(&self, schema: &Table) -> Result<Vec<u8>> {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        // Ensure the number of values matches the schema column count
        assert_eq!(self.values.len(), schema.col_count());

        if self.values.is_empty() {
            return Ok(vec![]);
        }

        let mut running_offset = schema.fixed_field_size_bytes();
        let mut variable_field_offsets = Vec::new();

        // First pass: Calculate offsets for variable-length fields
        for (i, column) in schema.columns().iter().enumerate() {
            match column.get_data_type() {
                DataType::Text => {
                    variable_field_offsets.push(running_offset);
                    // todo(eyoon): This should be incremented by the schema column size, not the field size
                    running_offset += self.values.get(i).unwrap().get_size();
                }
                _ => {}
            }
        }

        // Calculate total buffer size and initialize it
        let header_size = 2 * variable_field_offsets.len() as u16;
        let e2e_size_bytes = header_size + running_offset;
        let mut data = vec![0; e2e_size_bytes as usize];

        // Write header data to the buffer
        let mut cursor = 0_usize;
        for offset in variable_field_offsets.iter() {
            let dst = offset + header_size;
            let offset_bytes = dst.to_le_bytes();
            data[cursor..cursor + 2].copy_from_slice(&offset_bytes);
            assert_eq!(dst, u16::from_le_bytes([data[cursor], data[cursor + 1]]));
            cursor += 2;
        }

        // Write field data to the buffer
        let mut var_cursor =
            schema.fixed_field_size_bytes() as usize + 2 * variable_field_offsets.len();
        for (i, column) in schema.columns().iter().enumerate() {
            let field_bytes = self.values.get(i).unwrap().serialize();
            let num_bytes = field_bytes.len();
            match column.get_data_type() {
                DataType::Text => {
                    data[var_cursor..(var_cursor + num_bytes)].copy_from_slice(&field_bytes);
                    var_cursor += num_bytes;
                }
                _ => {
                    data[cursor..(cursor + num_bytes)].copy_from_slice(&field_bytes);
                    cursor += num_bytes;
                }
            }
        }

        Ok(data)
        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Deserializes a byte stream into a Row object.
    ///
    /// `bytes` contains u16 offsets for variable-length fields, followed
    /// by fixed-length fields, with variable-length fields at the end.
    pub fn deserialize(bytes: Vec<u8>, schema: &Table) -> Self {
        // Get the offsets of the variable length text fields, if any exist.
        let variable_field_offsets: Vec<u16> = (0..schema.variable_length_fields())
            .map(|i| u16::from_le_bytes([bytes[2 * i], bytes[(2 * i) + 1]]))
            .collect();

        // The first byte in `bytes` of the field data
        let field_data_start = variable_field_offsets.len() * 2;

        let values = schema
            .columns()
            .iter()
            .map(|column| match column.get_data_type() {
                DataType::Text => {
                    // Get the index into the variable length field offset array.
                    let offset_index = column.stored_offset() as usize;
                    let start = *variable_field_offsets.get(offset_index).unwrap() as usize;
                    let end = if offset_index == variable_field_offsets.len() - 1 {
                        bytes.len()
                    } else {
                        *variable_field_offsets.get(offset_index + 1).unwrap() as usize
                    };

                    // todo(eyoon): update deserialize based on chnages to to_bytes
                    Field::deserialize(&bytes[start..end], DataType::Text)
                }
                datatype => {
                    // Get the offset of the field in the byte stream.
                    let start = column.stored_offset() as usize + field_data_start;
                    let end = start + column.length_bytes() as usize;

                    Field::deserialize(&bytes[start..end], datatype)
                }
            })
            .collect();
        Self { values }
    }
}
// eof  ‎‎‎‎
