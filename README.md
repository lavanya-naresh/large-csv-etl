## **Python Programming Problem: Large CSV ETL and Database Loader**

### **Background**

You are provided with a very large CSV file (several GBs in size, larger than your available RAM) containing transaction data. The file has the following columns:
`transaction_id, user_id, amount, timestamp, status`

You need to process this file efficiently, transform some fields, and load the cleaned data into a SQLite database table. The process must be scalable, robust, and follow good coding practices.

### **Requirements**

1. **Chunked Reading:**
   Read the CSV file in manageable chunks (do not load the whole file into memory at once).

2. **Transformation:**
   For each chunk:

   * Standardize the `status` column (e.g., make all values lowercase).
   * Remove any rows where `amount` is negative or `status` is "cancelled".
   * Add a new column, `processed_at`, which contains the current UTC timestamp when the chunk is processed.

3. **Chunk Management:**

   * After transforming, write each processed chunk to a temporary file.
   * Ensure that, after all chunks are processed, you can reconstruct the entire processed dataset into a single new CSV file.

4. **Database Loading:**

   * Load the fully processed data into a specified table in a SQLite database, using efficient batch inserts.

5. **Parallelization:**

   * Use Python’s `concurrent.futures` or `asyncio` to process and write multiple chunks in parallel, optimizing for performance.

6. **Error Handling:**

   * Implement error handling to log and skip problematic chunks or rows, ensuring that processing continues even if some data is malformed.

7. **Testing:**

   * Write at least two automated test cases:

     * One for the transformation logic.
     * One for the database insertion.

8. **Project Structure:**

   * Organize your code into separate modules for:

     * File I/O and chunking
     * Data processing/transformation
     * Database connection and insertion
     * Logging and error handling
     * Testing

9. **Code Quality:**

   * Write readable, modular code with docstrings and comments as appropriate.
   * Follow PEP8 style guidelines.

### **Deliverables**

* The Python codebase, organized as described.
* A brief README with setup instructions and how to run the main pipeline and the tests.
* Example log output demonstrating error handling for at least one faulty chunk or row.

---

**Bonus:**
If you have time, add a command-line interface (CLI) to configure the file path, chunk size, and database location.