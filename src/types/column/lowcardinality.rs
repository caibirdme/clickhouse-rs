use chrono_tz::Tz;
use crate::{
    errors::{
        Error,
        Result,
        FromSqlError,
    },
    types::{
        column::{ArcColumnData, VectorColumnData},
        SqlType,
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
        size: usize,
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

impl ColumnData for LowCardinalityColumnData {
    fn len(&self) -> usize {
        self.key_data.len()
    }
    fn sql_type(&self) -> SqlType {
        SqlType::LowCardinality(self.index_data.sql_type().into())
    }
    fn at(&self, index: usize) -> crate::types::ValueRef {
        unimplemented!()
    }
    fn push(&mut self, value: crate::types::Value) {
        unimplemented!()
    }
    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        unimplemented!()
    }
    fn clone_instance(&self) -> super::column_data::BoxColumnData {
        unimplemented!()
    }
}