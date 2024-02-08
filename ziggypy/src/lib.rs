use pyo3::prelude::*;

#[pymodule]
#[pyo3(name="_rustypy")]
fn module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<IndexedStringCore>()?;
    Ok(())
}

#[pyclass]
struct IndexedStringCore {
    length: usize,
}

#[pymethods]
impl IndexedStringCore {
    #[new]
    fn new(filename: &str, length: usize) -> Self {
        Self {
            length
        }
    }

    fn __len__(&self) -> usize {
        self.length
    }
}
