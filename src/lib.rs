use pyo3::prelude::*;

mod dataframe;
mod session;
mod column;
mod schema;
mod execution;
mod functions;
mod expression;
mod lazy;
mod arrow_conversion;

use session::{SparkSession, SparkSessionBuilder, DataFrameReader};
use dataframe::{DataFrame, GroupedData};

/// Python module definition
#[pymodule]
fn _robin_sparkless(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<SparkSession>()?;
    m.add_class::<SparkSessionBuilder>()?;
    m.add_class::<DataFrameReader>()?;
    m.add_class::<DataFrame>()?;
    m.add_class::<GroupedData>()?;
    m.add_class::<column::Column>()?;
    m.add_class::<schema::StructType>()?;
    m.add_class::<schema::StructField>()?;
    
    // Add functions module
    let functions_module = PyModule::new(_py, "functions")?;
    functions::register_functions(_py, functions_module)?;
    m.add_submodule(functions_module)?;
    
    Ok(())
}
