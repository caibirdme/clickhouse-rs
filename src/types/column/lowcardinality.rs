use std::collections::hash_map::{HashMap, Entry};

use chrono_tz::Tz;
use crate::{
    errors::{
        Error,
        Result,
        FromSqlError,
    },
    types::{
        column::ArcColumnData,
        SqlType,
        Value,
        ValueRef,
    },
    
    binary::{Encoder, ReadEx},
};

use super::{ColumnData, ArcColumnWrapper};

pub(crate) struct LowCardinalityColumnData {
    pub(crate) index_data: ArcColumnData,
    pub(crate) key_data: ArcColumnData,
}

impl LowCardinalityColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        type_name: &str,
        _size: usize,
        tz: Tz,
    ) -> Result<Self> {
        const TYPE_MASK: u64 = 0b11111111;
        let idx_serialize_type = reader.read_scalar::<u64>()?;
        let idx_rows = reader.read_scalar::<u64>()?;
        let index_data = <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, idx_rows as usize, tz)?;
        let key_rows = reader.read_scalar::<u64>()? as usize;
        let key_data = match idx_serialize_type&TYPE_MASK {
            0 => <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, "UInt8", key_rows, tz)?,
            1 => <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, "UInt16", key_rows, tz)?,
            2 => <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, "UInt32", key_rows, tz)?,
            3 => <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, "UInt64", key_rows, tz)?,
            _ => Err(Error::FromSql(FromSqlError::UnsupportedOperation))?
        };
        Ok(Self{
            index_data,
            key_data,
        })
    }
}

impl From<ArcColumnData> for LowCardinalityColumnData {
    fn from(col_data: ArcColumnData) -> Self {
        let mut m = HashMap::new();
        let mut count = 0;
        let mut index = Vec::with_capacity(col_data.len());
        for i in 0..col_data.len() {
            let v_ref = col_data.at(i);
            match m.entry(&v_ref) {
                Entry::Occupied(c) => {
                    index[i] = *c.get();
                },
                Entry::Vacant(c) => {
                    c.insert(count);
                    index[i] = count;
                    count+=1;
                }
            }
        }
        
    }
}

fn into_index_column_data(arr: Vec<i32>) -> ArcColumnData {
    let n = arr.len();
    
}

impl ColumnData for LowCardinalityColumnData {
    fn len(&self) -> usize {
        self.key_data.len()
    }
    fn sql_type(&self) -> SqlType {
        SqlType::LowCardinality(self.index_data.sql_type().into())
    }
    fn at(&self, index: usize) -> ValueRef {
        unimplemented!()
    }
    fn push(&mut self, value: Value) {
        unimplemented!()
    }
    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        unimplemented!()
    }
    fn clone_instance(&self) -> super::column_data::BoxColumnData {
        unimplemented!()
    }
}