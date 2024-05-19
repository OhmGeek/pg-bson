use bson::Bson;
use bson::bson;

use pgrx::pg_sys::BYTEAARRAYOID;
use pgrx::prelude::*;
use pgrx::*;
use pgrx_sql_entity_graph::metadata::{Returns, SqlMapping, SqlTranslatable};

pgrx::pg_module_magic!();

// 1. Function that returns random BSON data for testing...
// 2. Function that takes in a byte array, and returns associated BSON. (allowing users to write BSON into the store)


#[derive(Debug)]
struct BsonValue {
    value: Bson
}

impl PostgresType for BsonValue {}

// Bson value should map in SQL as bson
unsafe impl SqlTranslatable for BsonValue {
    fn argument_sql() -> Result<pgrx_sql_entity_graph::metadata::SqlMapping, pgrx_sql_entity_graph::metadata::ArgumentError> {
        Ok(SqlMapping::literal("bson"))
    }

    fn return_sql() -> Result<pgrx_sql_entity_graph::metadata::Returns, pgrx_sql_entity_graph::metadata::ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("bson")))
    }
}

impl IntoDatum for BsonValue {
    fn into_datum(self) -> Option<pgrx::pg_sys::Datum> {
        // convert the BSON value into a byte array.
        let raw_bytes = bson::to_vec(&self.value);

        // Return the raw bytes, converted to postgres format.
        return raw_bytes.into_datum();
    }

    fn type_oid() -> pgrx::pg_sys::Oid {
        BYTEAARRAYOID
    }
}

impl FromDatum for BsonValue {
    unsafe fn from_polymorphic_datum(
        datum: pgrx::pg_sys::Datum,
        is_null: bool,
        _: pgrx::pg_sys::Oid,
    ) -> Option<BsonValue> {
        if is_null {
            None
        } else {
            
            // 1. We need to load the raw document from the Postgres Toast.
            let varlena = datum.cast_mut_ptr();
            let detoast = pgrx::pg_sys::pg_detoast_datum_packed(varlena);
            let len = varsize_any_exhdr(detoast);
            let data = vardata_any(detoast);

            let result = std::slice::from_raw_parts(data as *mut u8, len).to_owned();
            
            let from_slice = bson::from_slice(&result).unwrap();

            let output: BsonValue = BsonValue {
                value: from_slice 
            };

            if detoast != varlena {
                pgrx::pg_sys::pfree(detoast as void_mut_ptr);
            }

            // Return output
            Some(output)
        }
    }
}

#[pg_extern]
fn bson_document() -> BsonValue {
    let data = bson!({
        "name": "abc",
        "age": 43,
        "phones": [
            "0001",
            "0002"
        ]});
    
    BsonValue { value: data }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_bson_document() {
        let doc = crate::bson_document();
        assert_eq!("abc", doc.value.as_document().unwrap().get_str("name").unwrap())
    }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
