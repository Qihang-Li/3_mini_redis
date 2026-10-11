use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct Database {
    entries: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Database {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        let entries = self.entries.lock().expect("Database mutex is poisoned");
        // Use the borrowed `key` without constructing an owned `String`.
        entries.get(key).cloned()
    }

    /// Inserts a key-value pair, replacing any existing value for the key.
    ///
    /// # Panics
    /// Panics if the database mutex is poisoned, typically after a thread
    /// panics while holding the lock.
    pub fn set(&self, key: String, value: Bytes) {
        let mut entries = self.entries.lock().expect("Database mutex is poisoned");
        entries.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_database_new() {
        let database = Database::new();
        let entries = database.entries.lock().unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_database_get() {
        let database = Database::new();
        {
            database
                .entries
                .lock()
                .unwrap()
                .insert(String::from("Answer"), Bytes::from("42"));
        }

        // Retrieve an existing value
        let existing_value = database.get("Answer").unwrap();
        assert_eq!(existing_value, Bytes::from("42"));

        // Look up a missing key
        let missing_value = database.get("Solution");
        assert_eq!(missing_value, None);
    }

    #[test]
    fn test_database_set() {
        let database = Database::new();
        {
            database
                .entries
                .lock()
                .unwrap()
                .insert(String::from("Answer"), Bytes::from("42"));
        }

        // Overwrite an existing entry
        database.set(String::from("Answer"), Bytes::from("Forty-two"));
        // Release this lock before the next call to `set`.
        {
            let guard = database.entries.lock().unwrap();
            let updated_value = guard.get("Answer").unwrap();
            assert_eq!(*updated_value, Bytes::from("Forty-two"));
        } // Dropping `guard` releases the mutex.

        // Add a new entry
        database.set(String::from("Alpha"), Bytes::from("137"));
        {
            let guard = database.entries.lock().unwrap();
            let new_value = guard.get("Alpha").unwrap();
            assert_eq!(*new_value, Bytes::from("137"));
        }
    }
}
