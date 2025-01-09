
## Testing Framework Overview

### Synthetic Tests
- Synthetic tests use artificial symbols (`TEST1`, `TEST2`) to ensure clarity and avoid conflicts with real data.

### Real Data Workflow
- Real data tests use actual stock symbols from the SQLite database.

### Example Workflow
#### Synthetic Test
Synthetic prices:
| Date       | TEST1 | TEST2 |
|------------|-------|-------|
| 2023-01-01 | 100   | 200   |
| 2023-01-02 | 105   | 210   |
| 2023-01-03 | 110   | 220   |
