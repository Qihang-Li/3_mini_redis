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

    /// Retrieves a value from the database.
    ///
    /// # Panics
    /// Panics if the internal database lock is poisoned by a crashed thread.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        // Here we use a slice for key to optimize performance
        let data = self.rows.lock().unwrap();
        data.get(key).cloned()
    }

    /// Inserts a key-value pair into the database as an entry,
    /// overwriting the existing value if any.
    ///
    /// # Panics
    /// Panics if the internal database lock is poisoned by a crashed thread.
    pub fn set(&self, key: String, value: Bytes) {
        let mut data = self.rows.lock().unwrap();
        data.insert(key, value);
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::database::Database;

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

        // Test 1: get with a valid key
        let valid_value = test_db.get("Answer").unwrap();
        assert_eq!(valid_value, Bytes::from("42"));

        // Test 2: get with an invalid key
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

        // Test 1: set to overwrite an entry
        test_db.set(String::from("Answer"), Bytes::from("Forty-two"));
        // Scope block strictly isolates the lock for reading
        {
            let guard = test_db.rows.lock().unwrap();
            let overwrite_value = guard.get("Answer").unwrap();
            assert_eq!(*overwrite_value, Bytes::from("Forty-two"));
        } // guard is dropped here, unlocking the Mutex

        // Test 2: set to add an entry
        test_db.set(String::from("Alpha"), Bytes::from("137"));
        {
            let guard = test_db.rows.lock().unwrap();
            let new_value = guard.get("Alpha").unwrap();
            assert_eq!(*new_value, Bytes::from("137"));
        }
    }
}
