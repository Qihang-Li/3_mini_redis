use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct Database {
    rows: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Database {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retrieves the value associated with `key`.
    ///
    /// Returns `Some` containing a clone of the stored `Bytes`, or `None` if
    /// the key is absent. The clone shares the underlying byte storage.
    ///
    /// # Panics
    /// Panics if the database mutex is poisoned, typically after a thread
    /// panics while holding the lock.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let data = self.rows.lock().unwrap();
        // Use the borrowed `key` without constructing an owned `String`.
        data.get(key).cloned()
    }

    /// Inserts a key-value pair, replacing any existing value for the key.
    ///
    /// # Panics
    /// Panics if the database mutex is poisoned, typically after a thread
    /// panics while holding the lock.
    pub fn set(&self, key: String, value: Bytes) {
        let mut data = self.rows.lock().unwrap();
        data.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_database_new() {
        let test_db = Database::new();
        let test_data = test_db.rows.lock().unwrap();
        assert_eq!(test_data.len(), 0);
    }

    #[test]
    fn test_database_get() {
        let test_db = Database::new();
        {
            test_db
                .rows
                .lock()
                .unwrap()
                .insert(String::from("Answer"), Bytes::from("42"));
        }

        // Retrieve an existing value
        let valid_value = test_db.get("Answer").unwrap();
        assert_eq!(valid_value, Bytes::from("42"));

        // Look up a missing key
        let invalid_value = test_db.get("Solution");
        assert_eq!(invalid_value, None);
    }

    #[test]
    fn test_database_set() {
        let test_db = Database::new();
        {
            test_db
                .rows
                .lock()
                .unwrap()
                .insert(String::from("Answer"), Bytes::from("42"));
        }

        // Overwrite an existing entry
        test_db.set(String::from("Answer"), Bytes::from("Forty-two"));
        // Release this lock before the next call to `set`.
        {
            let guard = test_db.rows.lock().unwrap();
            let overwrite_value = guard.get("Answer").unwrap();
            assert_eq!(*overwrite_value, Bytes::from("Forty-two"));
        } // Dropping `guard` releases the mutex.

        // Add a new entry
        test_db.set(String::from("Alpha"), Bytes::from("137"));
        {
            let guard = test_db.rows.lock().unwrap();
            let new_value = guard.get("Alpha").unwrap();
            assert_eq!(*new_value, Bytes::from("137"));
        }
    }
}
