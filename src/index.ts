import dotenv from "dotenv";

dotenv.config({ override: true });
import Knex, { Knex as KnexType } from "knex";
import { select, checkbox, confirm, Separator } from "@inquirer/prompts";
import chalk from "chalk";
import ora from "ora";
import { Command } from "commander";
import * as fs from "fs";
import * as path from "path";

// ============== Types ==============
interface ColumnInfo {
  TABLE_SCHEMA: string;
  TABLE_NAME: string;
  COLUMN_NAME: string;
  DATA_TYPE: "timestamp" | "datetime";
  IS_NULLABLE: "YES" | "NO";
  COLUMN_DEFAULT: string | null;
  badRowCount: number;
}

interface TableSelection {
  schema: string;
  table: string;
}

interface CacheData {
  connection: {
    host: string;
    port: number;
  };
  timestamp: string;
  schemas: string[];
  columns: ColumnInfo[];
}

interface DbConfig {
  host: string;
  port: number;
  user: string;
  password: string;
}

type Action =
  | "scan"
  | "report"
  | "fix_nulls"
  | "allow_nulls"
  | "convert_timestamps"
  | "exit";

const CACHE_FILE = ".db-fixer-cache.json";

const SYSTEM_SCHEMAS = [
  "information_schema",
  "mysql",
  "performance_schema",
  "sys",
];

// ============== Configuration ==============
function getConfig(): DbConfig {
  const config: DbConfig = {
    host: process.env.DB_HOST || "127.0.0.1",
    port: Number(process.env.DB_PORT) || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD || "",
  };

  if (!config.password) {
    console.log(
      chalk.yellow(
        "⚠️  Warning: DB_PASSWORD is empty. Check your .env file.\n",
      ),
    );
  }

  return config;
}

function createKnex(config: DbConfig): KnexType {
  return Knex({
    client: "mysql2",
    connection: {
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    },
  });
}

// ============== Cache Functions ==============
function getCachePath(): string {
  return path.resolve(process.cwd(), CACHE_FILE);
}

function loadCache(config: DbConfig): CacheData | null {
  const cachePath = getCachePath();
  if (!fs.existsSync(cachePath)) {
    return null;
  }
  try {
    const data = fs.readFileSync(cachePath, "utf-8");
    const cache = JSON.parse(data) as CacheData;

    // Invalidate cache if connection changed
    if (
      cache.connection?.host !== config.host ||
      cache.connection?.port !== config.port
    ) {
      return null;
    }

    return cache;
  } catch {
    return null;
  }
}

function saveCache(
  config: DbConfig,
  schemas: string[],
  columns: ColumnInfo[],
): void {
  const cachePath = getCachePath();
  const data: CacheData = {
    connection: {
      host: config.host,
      port: config.port,
    },
    timestamp: new Date().toISOString(),
    schemas,
    columns,
  };
  fs.writeFileSync(cachePath, JSON.stringify(data, null, 2), "utf-8");
}

function formatCacheAge(timestamp: string): string {
  const diff = Date.now() - new Date(timestamp).getTime();
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0) return `${days} day(s) ago`;
  if (hours > 0) return `${hours} hour(s) ago`;
  if (minutes > 0) return `${minutes} minute(s) ago`;
  return "just now";
}

// ============== Database Queries ==============
async function getAllSchemas(knex: KnexType): Promise<string[]> {
  const result = await knex.raw("SHOW DATABASES");
  const databases: string[] = result[0].map(
    (row: { Database: string }) => row.Database,
  );

  return databases.filter((db) => !SYSTEM_SCHEMAS.includes(db));
}

async function getDateColumns(
  knex: KnexType,
  schemas: string[],
): Promise<Omit<ColumnInfo, "badRowCount">[]> {
  const columns = await knex("information_schema.COLUMNS")
    .select(
      "TABLE_SCHEMA",
      "TABLE_NAME",
      "COLUMN_NAME",
      "DATA_TYPE",
      "IS_NULLABLE",
      "COLUMN_DEFAULT",
    )
    .whereIn("TABLE_SCHEMA", schemas)
    .whereIn("DATA_TYPE", ["timestamp", "datetime"]);

  return columns;
}

async function countBadRows(
  knex: KnexType,
  schema: string,
  table: string,
  column: string,
): Promise<number> {
  try {
    const result = await knex.raw(
      `SELECT COUNT(*) as count FROM \`${schema}\`.\`${table}\` WHERE CAST(\`${column}\` AS CHAR) LIKE '0000-00-00%'`,
    );
    return result[0][0].count;
  } catch (error) {
    // Table might be locked or inaccessible
    return -1;
  }
}

async function fetchBadRowsSample(
  knex: KnexType,
  schema: string,
  table: string,
  column: string,
  limit: number = 10,
): Promise<Record<string, unknown>[]> {
  try {
    const result = await knex.raw(
      `SELECT * FROM \`${schema}\`.\`${table}\` WHERE CAST(\`${column}\` AS CHAR) LIKE '0000-00-00%' LIMIT ${limit}`,
    );
    return result[0];
  } catch (error) {
    return [];
  }
}

async function fixBadRows(
  schema: string,
  table: string,
  column: string,
  knex: KnexType,
): Promise<number> {
  await knex.raw("SET SESSION sql_mode = ''");

  const result = await knex.raw(
    `UPDATE \`${schema}\`.\`${table}\` SET \`${column}\` = NULL WHERE CAST(\`${column}\` AS CHAR) LIKE '0000-00-00%'`,
  );

  return result[0].affectedRows;
}

async function convertTimestampToDatetime(
  schema: string,
  table: string,
  column: string,
  isNullable: "YES" | "NO",
  knex: KnexType,
): Promise<void> {
  const nullableClause = isNullable === "YES" ? "NULL" : "NOT NULL";

  await knex.raw(
    `ALTER TABLE \`${schema}\`.\`${table}\` MODIFY COLUMN \`${column}\` DATETIME ${nullableClause}`,
  );
}

async function allowNullOnColumn(
  schema: string,
  table: string,
  column: string,
  dataType: "timestamp" | "datetime",
  knex: KnexType,
): Promise<void> {
  await knex.raw(
    `ALTER TABLE \`${schema}\`.\`${table}\` MODIFY COLUMN \`${column}\` ${dataType.toUpperCase()} NULL`,
  );
}

// ============== Schema Selection ==============
async function selectSchemas(knex: KnexType): Promise<string[]> {
  const spinner = ora("Fetching available schemas...").start();

  const allSchemas = await getAllSchemas(knex);

  spinner.succeed(`Found ${allSchemas.length} schemas.`);

  if (allSchemas.length === 0) {
    console.log(chalk.red("\nNo accessible schemas found!\n"));
    process.exit(1);
  }

  const selected = await checkbox({
    message: "Select schemas to scan:",
    choices: allSchemas.map((schema) => ({
      name: schema,
      value: schema,
      checked: true,
    })),
    required: true,
  });

  return selected;
}

// ============== Scanning Logic ==============
async function scanSchemas(
  knex: KnexType,
  schemas: string[],
): Promise<ColumnInfo[]> {
  const spinner = ora("Fetching date columns...").start();

  try {
    const columns = await getDateColumns(knex, schemas);
    spinner.text = `Found ${columns.length} date columns. Scanning for bad data...`;

    const results: ColumnInfo[] = [];
    let scanned = 0;

    for (const col of columns) {
      scanned++;
      spinner.text = `[${scanned}/${columns.length}] Scanning ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}...`;

      const badCount = await countBadRows(
        knex,
        col.TABLE_SCHEMA,
        col.TABLE_NAME,
        col.COLUMN_NAME,
      );

      results.push({
        ...col,
        badRowCount: badCount,
      });
    }

    spinner.succeed(
      `Scanned ${columns.length} columns across ${schemas.length} schema(s).`,
    );
    return results;
  } catch (error) {
    spinner.fail("Failed to scan schemas.");
    throw error;
  }
}

async function rescanColumns(
  knex: KnexType,
  existingColumns: ColumnInfo[],
  affectedColumns: ColumnInfo[],
): Promise<ColumnInfo[]> {
  if (affectedColumns.length === 0) {
    return existingColumns;
  }

  const spinner = ora(
    `Rescanning ${affectedColumns.length} affected column(s)...`,
  ).start();

  const affectedKeys = new Set(
    affectedColumns.map(
      (c) => `${c.TABLE_SCHEMA}.${c.TABLE_NAME}.${c.COLUMN_NAME}`,
    ),
  );

  const updatedColumns: ColumnInfo[] = [];
  let rescanned = 0;

  for (const col of existingColumns) {
    const key = `${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}`;

    if (affectedKeys.has(key)) {
      rescanned++;
      spinner.text = `[${rescanned}/${affectedColumns.length}] Rescanning ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}...`;

      // Re-fetch column info from information_schema for updated nullability
      const colInfo = await knex("information_schema.COLUMNS")
        .select("IS_NULLABLE", "DATA_TYPE")
        .where({
          TABLE_SCHEMA: col.TABLE_SCHEMA,
          TABLE_NAME: col.TABLE_NAME,
          COLUMN_NAME: col.COLUMN_NAME,
        })
        .first();

      const badCount = await countBadRows(
        knex,
        col.TABLE_SCHEMA,
        col.TABLE_NAME,
        col.COLUMN_NAME,
      );

      updatedColumns.push({
        ...col,
        IS_NULLABLE: colInfo?.IS_NULLABLE ?? col.IS_NULLABLE,
        DATA_TYPE: colInfo?.DATA_TYPE ?? col.DATA_TYPE,
        badRowCount: badCount,
      });
    } else {
      updatedColumns.push(col);
    }
  }

  spinner.succeed(`Rescanned ${affectedColumns.length} column(s).`);
  return updatedColumns;
}

// ============== Display Functions ==============
function displayResults(columns: ColumnInfo[]): void {
  // Group by schema
  const bySchema = columns.reduce(
    (acc, col) => {
      if (!acc[col.TABLE_SCHEMA]) {
        acc[col.TABLE_SCHEMA] = [];
      }
      acc[col.TABLE_SCHEMA].push(col);
      return acc;
    },
    {} as Record<string, ColumnInfo[]>,
  );

  for (const [schema, schemaCols] of Object.entries(bySchema)) {
    const badCount = schemaCols.filter((c) => c.badRowCount > 0).length;
    const timestampCount = schemaCols.filter(
      (c) => c.DATA_TYPE === "timestamp",
    ).length;

    console.log(
      "\n" +
        chalk.cyan.bold(`📁 ${schema}`) +
        chalk.gray(
          ` (${schemaCols.length} columns, ${badCount} with bad data, ${timestampCount} timestamps)`,
        ),
    );
    console.log(chalk.bold("─".repeat(100)));
    console.log(
      chalk.bold(
        "  Table.Column".padEnd(45) +
          "Type".padEnd(12) +
          "Nullable".padEnd(10) +
          "Bad Rows".padEnd(12) +
          "Status",
      ),
    );
    console.log(chalk.bold("─".repeat(100)));

    for (const col of schemaCols) {
      const name = `${col.TABLE_NAME}.${col.COLUMN_NAME}`.padEnd(45);
      const type = col.DATA_TYPE.padEnd(12);
      const nullable = col.IS_NULLABLE.padEnd(10);
      const badRows =
        col.badRowCount === -1
          ? "ERROR".padEnd(12)
          : String(col.badRowCount).padEnd(12);

      let status = chalk.green("✓ OK");
      if (col.badRowCount === -1) {
        status = chalk.red("✗ Could not scan");
      } else if (col.badRowCount > 0 && col.IS_NULLABLE === "NO") {
        status = chalk.red("⚠ Bad data, NOT NULLABLE");
      } else if (col.badRowCount > 0) {
        status = chalk.yellow("⚠ Has bad data");
      } else if (col.DATA_TYPE === "timestamp") {
        status = chalk.blue("◐ 2038 risk");
      }

      console.log(`  ${name}${type}${nullable}${badRows}${status}`);
    }
  }

  console.log();
}

function displaySummary(columns: ColumnInfo[], schemas: string[]): void {
  const totalBad = columns.filter((c) => c.badRowCount > 0).length;
  const totalTimestamps = columns.filter(
    (c) => c.DATA_TYPE === "timestamp",
  ).length;
  const totalUnfixable = columns.filter(
    (c) => c.badRowCount > 0 && c.IS_NULLABLE === "NO",
  ).length;
  const totalFixable = columns.filter(
    (c) => c.badRowCount > 0 && c.IS_NULLABLE === "YES",
  ).length;

  console.log(chalk.bold("\n📊 Summary"));
  console.log(chalk.gray("─".repeat(40)));
  console.log(`   Schemas:        ${chalk.cyan(schemas.length)}`);
  console.log(`   Total Columns:  ${chalk.cyan(columns.length)}`);
  console.log(
    `   Timestamps:     ${chalk.blue(totalTimestamps)} ${chalk.gray("(2038 risk)")}`,
  );
  console.log(
    `   Bad Data:       ${totalBad > 0 ? chalk.yellow(totalBad) : chalk.green(totalBad)}`,
  );
  console.log(
    `   ├─ Fixable:     ${totalFixable > 0 ? chalk.yellow(totalFixable) : chalk.green(totalFixable)}`,
  );
  console.log(
    `   └─ Unfixable:   ${totalUnfixable > 0 ? chalk.red(totalUnfixable) : chalk.green(totalUnfixable)}`,
  );
  console.log();
}

function displayUnfixable(columns: ColumnInfo[]): void {
  const unfixable = columns.filter(
    (c) => c.badRowCount > 0 && c.IS_NULLABLE === "NO",
  );

  if (unfixable.length === 0) {
    console.log(chalk.green("\n✓ No unfixable columns!\n"));
    return;
  }

  console.log(
    chalk.red(
      "\n⚠️  The following columns have bad data but do NOT allow NULL:",
    ),
  );
  console.log(
    chalk.red(
      "   You must either change the column to allow NULL, or provide a default value.\n",
    ),
  );

  for (const col of unfixable) {
    console.log(
      chalk.red(
        `   • ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}`,
      ) + chalk.gray(` (${col.badRowCount} bad rows)`),
    );
  }

  console.log();
}

// ============== TUI Menus ==============
function groupBySchemaAndTable(
  columns: ColumnInfo[],
): Map<string, Map<string, ColumnInfo[]>> {
  const result = new Map<string, Map<string, ColumnInfo[]>>();

  for (const col of columns) {
    if (!result.has(col.TABLE_SCHEMA)) {
      result.set(col.TABLE_SCHEMA, new Map());
    }
    const schemaMap = result.get(col.TABLE_SCHEMA)!;
    if (!schemaMap.has(col.TABLE_NAME)) {
      schemaMap.set(col.TABLE_NAME, []);
    }
    schemaMap.get(col.TABLE_NAME)!.push(col);
  }

  return result;
}

// Symbol to indicate user cancelled
const CANCELLED = Symbol("cancelled");

async function selectTablesGrouped(
  columns: ColumnInfo[],
  filterFn: (col: ColumnInfo) => boolean,
  message: string,
  getDescription: (cols: ColumnInfo[]) => string,
): Promise<TableSelection[] | typeof CANCELLED> {
  const filtered = columns.filter(filterFn);

  if (filtered.length === 0) {
    console.log(chalk.yellow("\nNo matching columns found.\n"));
    return [];
  }

  const grouped = groupBySchemaAndTable(filtered);

  // Build choices with schema separators
  type TableValue = { schema: string; table: string };
  const choices: Array<
    { name: string; value: TableValue; checked: boolean } | Separator
  > = [];

  for (const [schema, tables] of grouped) {
    const schemaColCount = Array.from(tables.values()).flat().length;
    choices.push(
      new Separator(
        chalk.cyan.bold(`📁 ${schema}`) +
          chalk.gray(` (${schemaColCount} columns in ${tables.size} tables)`),
      ),
    );

    for (const [table, cols] of tables) {
      const desc = getDescription(cols);
      choices.push({
        name: `   ${table}` + chalk.gray(` ${desc}`),
        value: { schema, table },
        checked: true,
      });
    }
  }

  const selection = await checkbox<TableValue>({
    message,
    choices,
  });

  if (selection.length === 0) {
    const confirmEmpty = await select<"proceed" | "cancel">({
      message: "No tables selected. What would you like to do?",
      choices: [
        { name: "❌ Cancel and go back", value: "cancel" },
        { name: "✅ Proceed anyway (no changes)", value: "proceed" },
      ],
    });
    if (confirmEmpty === "cancel") {
      return CANCELLED;
    }
  }

  return selection;
}

async function selectAction(columns: ColumnInfo[]): Promise<Action> {
  const fixableCount = columns.filter(
    (c) => c.badRowCount > 0 && c.IS_NULLABLE === "YES",
  ).length;
  const unfixableCount = columns.filter(
    (c) => c.badRowCount > 0 && c.IS_NULLABLE === "NO",
  ).length;
  const timestampCount = columns.filter(
    (c) => c.DATA_TYPE === "timestamp",
  ).length;
  const hasCache = columns.length > 0;

  const choices: {
    name: string;
    value: Action;
    disabled?: boolean | string;
  }[] = [
    { name: "🔍 Scan databases (fresh scan)", value: "scan" },
    {
      name: `📊 View full report${hasCache ? ` (${columns.length} columns)` : ""}`,
      value: "report",
      disabled: !hasCache ? "scan first" : false,
    },
    {
      name: `🔧 Fix zero dates → NULL${fixableCount > 0 ? chalk.yellow(` (${fixableCount} fixable)`) : ""}`,
      value: "fix_nulls",
      disabled:
        fixableCount === 0 ? (hasCache ? "none found" : "scan first") : false,
    },
    {
      name: `🔓 Allow NULL on columns${unfixableCount > 0 ? chalk.red(` (${unfixableCount} unfixable)`) : ""}`,
      value: "allow_nulls",
      disabled:
        unfixableCount === 0 ? (hasCache ? "none found" : "scan first") : false,
    },
    {
      name: `🔄 Convert TIMESTAMP → DATETIME${timestampCount > 0 ? chalk.blue(` (${timestampCount} columns)`) : ""}`,
      value: "convert_timestamps",
      disabled:
        timestampCount === 0 ? (hasCache ? "none found" : "scan first") : false,
    },
    { name: "🚪 Exit", value: "exit" },
  ];

  const action = await select<Action>({
    message: "What would you like to do?",
    choices,
  });

  return action;
}

// ============== Execute Fixes ==============
type FixAction = "confirm" | "show_sample" | "cancel";

async function executeNullFixes(
  knex: KnexType,
  columns: ColumnInfo[],
  selectedTables: TableSelection[],
): Promise<ColumnInfo[]> {
  const tableSet = new Set(selectedTables.map((t) => `${t.schema}.${t.table}`));
  const toFix = columns.filter(
    (c) =>
      c.badRowCount > 0 &&
      c.IS_NULLABLE === "YES" &&
      tableSet.has(`${c.TABLE_SCHEMA}.${c.TABLE_NAME}`),
  );

  if (toFix.length === 0) {
    console.log(chalk.yellow("\nNo columns to fix in selected tables.\n"));
    return [];
  }

  console.log(chalk.yellow("\nColumns to fix:"));
  for (const col of toFix) {
    console.log(
      chalk.gray(
        `   • ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME} (${col.badRowCount} rows)`,
      ),
    );
  }

  // Action loop - allow showing samples before confirming
  let shouldProceed = false;
  while (!shouldProceed) {
    const action = await select<FixAction>({
      message: `What would you like to do? (${toFix.length} column(s) selected)`,
      choices: [
        { name: "✅ Proceed with fix", value: "confirm" },
        { name: "👁️  Show sample bad rows", value: "show_sample" },
        { name: "❌ Cancel", value: "cancel" },
      ],
    });

    if (action === "cancel") {
      console.log(chalk.yellow("Cancelled."));
      return [];
    }

    if (action === "show_sample") {
      // Show sample bad rows for each column
      const spinner = ora("Fetching sample rows...").start();
      for (const col of toFix) {
        spinner.text = `Fetching from ${col.TABLE_SCHEMA}.${col.TABLE_NAME}...`;
        const samples = await fetchBadRowsSample(
          knex,
          col.TABLE_SCHEMA,
          col.TABLE_NAME,
          col.COLUMN_NAME,
          5,
        );
        spinner.stop();

        console.log(
          chalk.cyan(
            `\n📋 ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME} (showing up to 5 of ${col.badRowCount}):`,
          ),
        );
        if (samples.length === 0) {
          console.log(chalk.gray("   No rows found or access denied."));
        } else {
          console.table(samples);
        }
        spinner.start();
      }
      spinner.stop();
      continue;
    }

    if (action === "confirm") {
      shouldProceed = true;
    }
  }

  const spinner = ora("Starting fixes...").start();
  const fixed: ColumnInfo[] = [];
  let totalAffected = 0;

  try {
    for (let i = 0; i < toFix.length; i++) {
      const col = toFix[i];
      spinner.text = `[${i + 1}/${toFix.length}] Fixing ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}...`;
      const affected = await fixBadRows(
        col.TABLE_SCHEMA,
        col.TABLE_NAME,
        col.COLUMN_NAME,
        knex,
      );
      totalAffected += affected;
      fixed.push(col);
    }

    spinner.succeed(
      chalk.green(
        `All ${toFix.length} fixes applied successfully! (${totalAffected} total rows updated)`,
      ),
    );
    return fixed;
  } catch (error) {
    spinner.fail(
      chalk.red(`Fix failed at column ${fixed.length + 1}/${toFix.length}.`),
    );
    console.error(error);
    return fixed;
  }
}

async function executeAllowNulls(
  knex: KnexType,
  columns: ColumnInfo[],
  selectedTables: TableSelection[],
): Promise<ColumnInfo[]> {
  const tableSet = new Set(selectedTables.map((t) => `${t.schema}.${t.table}`));
  const toAllow = columns.filter(
    (c) =>
      c.badRowCount > 0 &&
      c.IS_NULLABLE === "NO" &&
      tableSet.has(`${c.TABLE_SCHEMA}.${c.TABLE_NAME}`),
  );

  if (toAllow.length === 0) {
    console.log(
      chalk.yellow("\nNo columns need NULL allowance in selected tables.\n"),
    );
    return [];
  }

  console.log(chalk.yellow("\nColumns to modify (allow NULL):"));
  for (const col of toAllow) {
    console.log(
      chalk.gray(
        `   • ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME} (${col.badRowCount} bad rows)`,
      ),
    );
  }

  const confirmed = await confirm({
    message: `This will ALTER ${toAllow.length} column(s) to allow NULL. Continue?`,
    default: false,
  });

  if (!confirmed) {
    console.log(chalk.yellow("Aborted."));
    return [];
  }

  const spinner = ora("Starting modifications...").start();
  const allowed: ColumnInfo[] = [];

  try {
    for (let i = 0; i < toAllow.length; i++) {
      const col = toAllow[i];
      spinner.text = `[${i + 1}/${toAllow.length}] Allowing NULL on ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}...`;
      await allowNullOnColumn(
        col.TABLE_SCHEMA,
        col.TABLE_NAME,
        col.COLUMN_NAME,
        col.DATA_TYPE,
        knex,
      );
      allowed.push(col);
    }

    spinner.succeed(
      chalk.green(`All ${toAllow.length} columns modified successfully!`),
    );
    return allowed;
  } catch (error) {
    spinner.fail(
      chalk.red(
        `Modification failed at column ${allowed.length + 1}/${toAllow.length}.`,
      ),
    );
    console.error(error);
    return allowed;
  }
}

async function executeTimestampConversions(
  knex: KnexType,
  columns: ColumnInfo[],
  selectedTables: TableSelection[],
): Promise<ColumnInfo[]> {
  const tableSet = new Set(selectedTables.map((t) => `${t.schema}.${t.table}`));
  const toConvert = columns.filter(
    (c) =>
      c.DATA_TYPE === "timestamp" &&
      tableSet.has(`${c.TABLE_SCHEMA}.${c.TABLE_NAME}`),
  );

  if (toConvert.length === 0) {
    console.log(chalk.yellow("\nNo TIMESTAMP columns in selected tables.\n"));
    return [];
  }

  const hasProblems = toConvert.some((c) => c.badRowCount > 0);

  if (hasProblems) {
    console.log(
      chalk.red("\n⚠️  Some selected columns have bad data (0000-00-00)."),
    );
    console.log(
      chalk.red("   Fix the bad data FIRST, then convert to DATETIME.\n"),
    );

    const proceed = await confirm({
      message: "Proceed anyway? (May cause errors)",
      default: false,
    });

    if (!proceed) return [];
  }

  console.log(chalk.yellow("\nColumns to convert:"));
  for (const col of toConvert) {
    console.log(
      chalk.gray(
        `   • ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}`,
      ),
    );
  }

  const confirmed = await confirm({
    message: `This will ALTER ${toConvert.length} column(s) from TIMESTAMP to DATETIME. Continue?`,
    default: false,
  });

  if (!confirmed) {
    console.log(chalk.yellow("Aborted."));
    return [];
  }

  const spinner = ora("Starting conversion...").start();
  const converted: ColumnInfo[] = [];

  try {
    for (let i = 0; i < toConvert.length; i++) {
      const col = toConvert[i];
      spinner.text = `[${i + 1}/${toConvert.length}] Converting ${col.TABLE_SCHEMA}.${col.TABLE_NAME}.${col.COLUMN_NAME}...`;
      await convertTimestampToDatetime(
        col.TABLE_SCHEMA,
        col.TABLE_NAME,
        col.COLUMN_NAME,
        col.IS_NULLABLE,
        knex,
      );
      converted.push(col);
    }

    spinner.succeed(
      chalk.green(
        `All ${toConvert.length} conversions completed successfully!`,
      ),
    );
    return converted;
  } catch (error) {
    spinner.fail(
      chalk.red(
        `Conversion failed at column ${converted.length + 1}/${toConvert.length}.`,
      ),
    );
    console.error(error);
    return converted; // Return what was successfully converted
  }
}

// ============== Main ==============
async function main(): Promise<void> {
  const program = new Command();

  program
    .name("db-date-fixer")
    .description(
      "MySQL Date Fixer Tool - Fix zero dates and 2038 timestamp issues",
    )
    .option(
      "-a, --action <action>",
      "Action to perform: scan, report, fix_nulls, allow_nulls, convert_timestamps",
    )
    .parse(process.argv);

  const options = program.opts<{ action?: string }>();

  const config = getConfig();
  const knex = createKnex(config);

  console.log(chalk.cyan.bold("\n🔍 MySQL Date Fixer Tool"));
  console.log(chalk.gray(`   Host: ${config.host}:${config.port}`));
  console.log(chalk.gray(`   User: ${config.user}\n`));

  // Load cache if available
  const cache = loadCache(config);
  let columns: ColumnInfo[] = [];
  let selectedSchemas: string[] = [];

  if (cache) {
    console.log(
      chalk.gray(
        `📦 Found cached scan from ${formatCacheAge(cache.timestamp)}`,
      ),
    );
    console.log(chalk.gray(`   Schemas: ${cache.schemas.join(", ")}`));
    console.log(chalk.gray(`   Columns: ${cache.columns.length}\n`));

    columns = cache.columns;
    selectedSchemas = cache.schemas;
  }

  let running = true;

  while (running) {
    // Get action from CLI or prompt
    let action: Action;
    if (options.action) {
      action = options.action as Action;
      options.action = undefined; // Clear so we prompt next time
    } else {
      displaySummary(columns, selectedSchemas);
      action = await selectAction(columns);
    }

    switch (action) {
      case "scan": {
        selectedSchemas = await selectSchemas(knex);
        columns = await scanSchemas(knex, selectedSchemas);
        saveCache(config, selectedSchemas, columns);
        console.log(chalk.gray(`\n📦 Cached scan results to ${CACHE_FILE}\n`));
        break;
      }

      case "report":
        if (columns.length === 0) {
          console.log(chalk.yellow("\nNo scan data. Please scan first.\n"));
        } else {
          displayResults(columns);
        }
        break;

      case "fix_nulls": {
        if (columns.length === 0) {
          console.log(chalk.yellow("\nNo scan data. Please scan first.\n"));
          break;
        }
        const fixable = columns.filter(
          (c) => c.badRowCount > 0 && c.IS_NULLABLE === "YES",
        );
        if (fixable.length === 0) {
          console.log(chalk.yellow("\nNo fixable columns found.\n"));
          break;
        }
        const selected = await selectTablesGrouped(
          columns,
          (c) => c.badRowCount > 0 && c.IS_NULLABLE === "YES",
          "Select tables with fixable zero dates:",
          (cols) =>
            `(${cols.reduce((sum, c) => sum + c.badRowCount, 0)} bad rows in ${cols.length} columns)`,
        );
        if (selected === CANCELLED) {
          console.log(chalk.yellow("Cancelled.\n"));
          break;
        }
        const affected = await executeNullFixes(knex, columns, selected);
        if (affected.length > 0) {
          columns = await rescanColumns(knex, columns, affected);
          saveCache(config, selectedSchemas, columns);
        }
        break;
      }

      case "allow_nulls": {
        if (columns.length === 0) {
          console.log(chalk.yellow("\nNo scan data. Please scan first.\n"));
          break;
        }
        const unfixable = columns.filter(
          (c) => c.badRowCount > 0 && c.IS_NULLABLE === "NO",
        );
        if (unfixable.length === 0) {
          console.log(chalk.green("\n✓ No unfixable columns!\n"));
          break;
        }
        const selected = await selectTablesGrouped(
          columns,
          (c) => c.badRowCount > 0 && c.IS_NULLABLE === "NO",
          "Select tables to allow NULL (currently NOT NULLABLE with bad data):",
          (cols) =>
            `(${cols.reduce((sum, c) => sum + c.badRowCount, 0)} bad rows in ${cols.length} columns)`,
        );
        if (selected === CANCELLED) {
          console.log(chalk.yellow("Cancelled.\n"));
          break;
        }
        const affected = await executeAllowNulls(knex, columns, selected);
        if (affected.length > 0) {
          columns = await rescanColumns(knex, columns, affected);
          saveCache(config, selectedSchemas, columns);
        }
        break;
      }

      case "convert_timestamps": {
        if (columns.length === 0) {
          console.log(chalk.yellow("\nNo scan data. Please scan first.\n"));
          break;
        }
        const timestamps = columns.filter((c) => c.DATA_TYPE === "timestamp");
        if (timestamps.length === 0) {
          console.log(chalk.yellow("\nNo TIMESTAMP columns found.\n"));
          break;
        }
        const selected = await selectTablesGrouped(
          columns,
          (c) => c.DATA_TYPE === "timestamp",
          "Select tables to convert TIMESTAMP → DATETIME:",
          (cols) => {
            const badCount = cols.filter((c) => c.badRowCount > 0).length;
            return badCount > 0
              ? chalk.red(
                  `(${cols.length} columns, ${badCount} with bad data!)`,
                )
              : chalk.green(`(${cols.length} columns, clean)`);
          },
        );
        if (selected === CANCELLED) {
          console.log(chalk.yellow("Cancelled.\n"));
          break;
        }
        const affected = await executeTimestampConversions(
          knex,
          columns,
          selected,
        );
        if (affected.length > 0) {
          columns = await rescanColumns(knex, columns, affected);
          saveCache(config, selectedSchemas, columns);
        }
        break;
      }

      case "exit":
        running = false;
        break;
    }
  }

  console.log(chalk.gray("\nGoodbye! 👋\n"));
  await knex.destroy();
  process.exit(0);
}

main().catch((err) => {
  console.error(chalk.red("Fatal error:"), err);
  process.exit(1);
});
