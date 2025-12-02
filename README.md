
# External Access Audit Assets

This package contains:

- **notebooks/ExternalAccessAudit.ipynb** — PySpark notebook to filter `activityEventEntities` by date range and external-access operations, and to persist summary tables for reporting.
- **schemas/powerbi_report_schema.json** — A declarative schema describing the intended Power BI tables and visuals.
- **sql/filter_external_access.sql** — SQL queries to filter by date range and produce summaries.

## How to Use (Fabric Lakehouse)
1. **Upload** the notebook to your Fabric workspace and open it.
2. Update `lakehouse_path`, `start_date`, and `end_date` in the first cell. Run all cells.
3. Confirm Delta outputs at:
   - `/lakehouse/audit/summary_actor`
   - `/lakehouse/audit/summary_operation`
4. (Optional) The notebook registers tables `ActorSummary` and `OperationSummary` for easier SQL access.

## Build the Power BI Report
1. In **Power BI Desktop** (or Fabric), connect to your Lakehouse (Direct Lake or SQL endpoint).
2. Load the tables `ActorSummary` and `OperationSummary`.
3. Use `schemas/powerbi_report_schema.json` as guidance to create:
   - Bar chart: External Access by Actor (Actor vs ExternalAccessCount)
   - Column chart: External Access by Operation (Operation vs AccessCount)

## Notes
- **Date Range:** The notebook uses `Timestamp BETWEEN start_date AND end_date` (inclusive). Ensure `Timestamp` is UTC ISO-8601.
- **Operations:** External access indicators include `AcceptExternalDataShare`, `AddExternalResource`, `AddLinkToExternalResource`, and `AnalyzedByExternalApplication`.
- **Sources**: See Microsoft documentation for the audit operation list and Activity Events API.
