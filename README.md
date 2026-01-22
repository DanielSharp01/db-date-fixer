# MySQL Date Fixer Tool

A CLI tool to identify and fix common MySQL date/timestamp issues, including zero dates (`0000-00-00`) and the year 2038 timestamp overflow problem.

## Features

- **Smart Scanning**: Scans all date/datetime/timestamp columns across multiple schemas
- **Cached Results**: Saves scan results with connection awareness - automatically invalidates when switching databases
- **Multiple Fix Strategies**:
  - Fix bad data by setting to NULL (for nullable columns)
  - Allow NULL on columns (ALTER TABLE)
  - Convert TIMESTAMP to DATETIME (solves 2038 problem)
- **Interactive Workflows**:
  - Preview sample bad rows before fixing
  - Group tables by schema for easy selection
  - Progress tracking for all operations
  - Contextual action menu showing available fixes
- **Safe Operations**: Per-column execution with partial success handling

## Prerequisites

- Node.js 18+
- MySQL database access
- TypeScript (dev)

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Copy `.env.sample` to `.env` and configure your database connection:
   ```bash
   cp .env.sample .env
   ```
4. Edit `.env` with your MySQL credentials:
   ```
   DB_HOST=127.0.0.1
   DB_PORT=3306
   DB_USER=root
   DB_PASSWORD=your_password
   ```

## Usage

### Interactive Mode

Run the tool without arguments to enter interactive mode:

```bash
npm start
```

This will:
1. Load cached scan (if available and for the same database)
2. Present an action menu with available operations
3. Guide you through the selected operation

### Direct Action Mode

Specify an action directly via CLI:

```bash
npm start -- --action scan              # Scan database for issues
npm start -- --action report            # Show detailed report
npm start -- --action fix_nulls         # Fix bad data (set to NULL)
npm start -- --action allow_nulls       # ALTER columns to allow NULL
npm start -- --action convert_timestamps # Convert TIMESTAMP to DATETIME
```

## How It Works

### Scanning

The tool scans all `DATE`, `DATETIME`, and `TIMESTAMP` columns across your selected schemas, looking for:
- Zero dates (`0000-00-00 00:00:00` or `0000-00-00`)
- Counts of bad rows per column
- Column nullability and data type

Results are cached to `.db-fixer-cache.json` for quick access.

### Fixing Bad Data

For nullable columns with bad data:
- Updates rows to set the column to `NULL` where the value is `0000-00-00%`
- Shows progress `[n/N]` for trackability
- Allows previewing sample bad rows before proceeding

### Allowing NULL

For NOT NULL columns with bad data:
- Executes `ALTER TABLE ... MODIFY COLUMN ... NULL`
- Preserves data type
- Updates cache after successful modifications

### Converting TIMESTAMP to DATETIME

Solves the MySQL TIMESTAMP year 2038 overflow problem:
- Converts `TIMESTAMP` columns to `DATETIME`
- Preserves nullability
- **Note**: Check your code for timezone handling - TIMESTAMP stores UTC, DATETIME does not

## Safety Considerations

- **Transactions**: Each ALTER/UPDATE is atomic (no global transaction wrapping)
- **Partial Success**: If an operation fails partway, successfully modified columns are tracked
- **Cache Validation**: Cache is automatically invalidated when connecting to a different database
- **Preview Mode**: View sample bad rows before committing to fixes
- **TIMESTAMP Conversion**: Be aware of timezone implications when converting to DATETIME

## Cache Management

- Cache file: `.db-fixer-cache.json`
- Stores: connection info (host/port), scan timestamp, schemas, and column data
- Automatically invalidates when:
  - Connecting to different host/port
  - Manual deletion of cache file
- Re-scan anytime via the "Scan database" action

## Troubleshooting

### "No accessible schemas found"
- Check your database user permissions
- Ensure `DB_HOST`, `DB_PORT`, `DB_USER`, and `DB_PASSWORD` are correct

### Slow performance on large datasets
- Use the cache to avoid re-scanning
- Progress indicators show `[current/total]` for tracking
- Per-column execution (no global transaction) improves speed

### TIMESTAMP conversion considerations
- Verify your application handles timezone differences
- Check for `ON UPDATE CURRENT_TIMESTAMP` behavior
- Test in development before production use

## Development

Type check:
```bash
npm run typecheck
```

## License

MIT
