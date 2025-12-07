# Credit Card Transaction Processing - Business Logic Documentation

## Package Overview

**Package Name:** CreditCardTransactionProcessing
**Version:** 2.0.0
**Author:** Enterprise Data Engineering Team
**Environment:** PRODUCTION
**Schedule:** Daily at 2:00 AM EST
**SLA:** Complete within 4 hours
**Retention:** 7 years per regulatory requirement

**Purpose:**
This package processes daily credit card transactions through multiple stages including validation, fraud detection, compliance checking, and final staging for downstream reporting and analytics.

---

## Connection Managers

The package uses **7 connection managers**:

1. **SRC_CreditCardStaging** (OLEDB)
   - Server: SQLPROD-STG-01
   - Database: CreditCardStaging
   - Purpose: Source database for raw transactions

2. **TGT_DataWarehouse** (OLEDB)
   - Server: SQLPROD-DW-01
   - Database: CreditCardDW
   - Purpose: Target data warehouse

3. **FRAUD_DetectionDB** (OLEDB)
   - Server: SQLPROD-FRAUD-01
   - Database: FraudDetection
   - Purpose: Fraud detection processing

4. **COMPLIANCE_DB** (OLEDB)
   - Server: SQLPROD-COMP-01
   - Database: ComplianceDB
   - Purpose: Compliance checking (OFAC, AML)

5. **LOG_ErrorDatabase** (OLEDB)
   - Server: SQLPROD-LOG-01
   - Database: ETLLogging
   - Purpose: Error and execution logging

6. **SMTP_Notifications** (SMTP)
   - Server: smtp.enterprise.bank
   - Purpose: Email notifications

7. **FF_ArchiveExport** (FLATFILE)
   - Format: Pipe-delimited, UTF-8
   - Purpose: Archive export to file system
   - Dynamic path: `\\enterprise-nas\etl\creditcard\archive\CC_Archive_YYYYMMDD.txt`

---

## Package Variables

### Execution Control Variables
- **PackageExecutionID** (Int32): Tracks execution instance (default: 0)
- **ProcessingDate** (DateTime): Date to process (default: 2025-12-06)
- **BatchSize** (Int32): Batch size for processing (default: 50,000)

### Threshold Variables
- **HighValueThreshold** (Decimal): $10,000.00 - Triggers enhanced due diligence
- **FraudScoreThreshold** (Decimal): 0.75 - Fraud scoring threshold
- **DuplicateWindowMinutes** (Int32): 5 minutes - Duplicate detection window

### Counter Variables
- **TotalRecordsProcessed** (Int32): Count of processed records
- **TotalRecordsFailed** (Int32): Count of failed records
- **FraudAlertCount** (Int32): Count of fraud alerts generated
- **ComplianceViolationCount** (Int32): Count of compliance violations

### Path Variables
- **ArchiveFilePath** (String): `\\enterprise-nas\etl\creditcard\archive`
- **ErrorFilePath** (String): `\\enterprise-nas\etl\creditcard\errors`

### Email Variables
- **AlertEmailRecipients** (String): `dataengineering@enterprise.bank;compliance@enterprise.bank;fraud-alerts@enterprise.bank`

### Feature Flags
- **EnableFraudDetection** (Boolean): TRUE - Enable/disable fraud detection
- **EnableComplianceCheck** (Boolean): TRUE - Enable/disable compliance checks
- **EnableArchiving** (Boolean): TRUE - Enable/disable archiving

---

## Package Parameters

1. **ProcessingDate** (DateTime): Processing date parameter
2. **BatchSize** (Int32): Batch size (default: 50,000)
3. **Environment** (String): Environment name (default: "PROD")
4. **EnableNotifications** (Boolean): Enable email notifications

---

## Control Flow Tasks (Execution Order)

### STAGE 1: SEQ_Initialization
Initializes package execution context

**Tasks:**
1. **SQL_LogPackageStart**
   - Logs package execution start to `ETL_PackageExecution` table
   - Captures: PackageName, StartTime, Status, Environment, ProcessingDate, Server, User
   - Returns ExecutionID to PackageExecutionID variable

2. **SQL_ValidateConnections**
   - Validates database connections are accessible
   - Query: `SELECT 1 AS ConnectionValid`

3. **SQL_GetProcessingStats**
   - Gets count of pending transactions to process
   - Queries unprocessed records from `RawTransactions` table
   - Filters by ProcessedFlag = 0 and ProcessingDate range

**Execution Order:** LogPackageStart → ValidateConnections → GetProcessingStats

---

### STAGE 2: DFT_ExtractAndValidate
Data Flow Task for extraction and validation

#### Source:
**OLE_SRC_RawTransactions**
- Extracts from: `dbo.RawTransactions` (joined with `dbo.CustomerAccounts`)
- Filters: ProcessedFlag = 0, TransactionDateTime >= yesterday
- **Extracted Fields (31 fields):**
  - Transaction details: TransactionID, CardNumber, CardHolderName, TransactionAmount, CurrencyCode, TransactionDateTime
  - Merchant info: MerchantID, MerchantName, MerchantCategoryCode, MerchantCountryCode
  - Authorization: AuthorizationCode, ResponseCode, TerminalID
  - Security: CardEntryMode, CardPresentIndicator, CVVVerificationResult, AVSResponseCode
  - Digital: CustomerIPAddress, DeviceFingerprint, ProcessingChannel
  - Settlement: SettlementDate, InterchangeFee, NetworkID
  - Customer: CustomerID, CustomerSegment, AccountOpenDate, CreditLimit, CurrentBalance, AvailableCredit, RiskRating, LastActivityDate

#### Transformations:

**1. DER_AddCalculatedFields (Derived Column)**
- **MaskedCardNumber**: `LEFT(CardNumber,4) + "********" + RIGHT(CardNumber,4)` (PCI-DSS compliant)
- **IsHighValueTransaction**: `TransactionAmount >= 10000 ? TRUE : FALSE`
- **IsInternational**: `MerchantCountryCode != "USA" ? TRUE : FALSE`
- **ETLProcessingTimestamp**: `GETDATE()`
- **TransactionDateKey**: `YEAR * 10000 + MONTH * 100 + DAY` (Surrogate key)
- **TransactionHour**: `DATEPART("Hour", TransactionDateTime)`
- **CardBIN**: `LEFT(CardNumber,6)` (First 6 digits)
- **PackageExecutionID**: Links to execution tracking

**2. DCV_DataTypeConversion (Data Conversion)**
- Converts TransactionAmount from currency to decimal(18,4) for target compatibility

**3. CSPL_RouteByTransactionType (Conditional Split)**
- **Route 1 - HighValueTransactions**: `IsHighValueTransaction == TRUE`
- **Route 2 - InternationalTransactions**: `IsInternational == TRUE AND IsHighValueTransaction == FALSE`
- **Route 3 - StandardTransactions**: Default route

**4. LKP_MerchantCategoryValidation (Lookup)**
- Validates MerchantCategoryCode against approved list
- Reference Query: `SELECT MCC, CategoryDescription, RiskLevel, IsRestricted FROM dbo.MerchantCategoryCodes WHERE IsActive = 1`
- Lookup Join: MerchantCategoryCode = MCC
- Returns: CategoryDescription, MCCRiskLevel, IsRestrictedMCC
- **No Match Behavior**: Routes to quarantine

**5. RC_CountProcessedRecords (Row Count)**
- Updates TotalRecordsProcessed variable

**6. MCT_MultipleDestinations (Multicast)**
- Sends valid transactions to 3 destinations simultaneously

#### Destinations:

1. **OLE_DST_FactTransactions** (Data Warehouse)
   - Target: `[dbo].[FactCreditCardTransactions]`
   - Load Options: TABLOCK, CHECK_CONSTRAINTS, ROWS_PER_BATCH=50000
   - Fast Load mode enabled

2. **OLE_DST_FraudAnalysisStaging** (Fraud Detection)
   - Target: `[dbo].[FraudAnalysisQueue]`
   - Stages transactions for fraud analysis

3. **OLE_DST_QuarantineRecords** (Error Handling)
   - Target: `[dbo].[TransactionQuarantine]`
   - Receives invalid records (failed validation)

**Data Flow Path:**
```
Source → Derived Column → Data Conversion → Conditional Split
                                                    ↓
                                    Standard → Lookup → Match → Multicast → [DW, Fraud, Archive]
                                                        ↓
                                                   No Match → Quarantine
```

---

### STAGE 3: SEQ_FraudDetection
Executes fraud detection algorithms

**Business Rules Implemented:**

**1. SQL_ExecuteFraudScoring**
- Executes stored procedure: `dbo.usp_ProcessFraudDetection`
- Parameters: ProcessingDate, FraudScoreThreshold (0.75), PackageExecutionID
- Returns count of fraud alerts to FraudAlertCount variable
- Uses ML-based fraud scoring engine

**2. SQL_VelocityCheck**
- **Rule**: Detects cards with 5+ transactions within 5-minute window
- **Alert Type**: 'VELOCITY'
- **Risk Score**: 0.85
- **Logic**:
  ```sql
  Flags cards where multiple transactions (5+) occur
  within DuplicateWindowMinutes (5 mins) time window
  ```
- Inserts alerts into `dbo.FraudAlerts` table

**3. SQL_GeoAnomalyCheck**
- **Rule**: Detects impossible travel patterns
- **Alert Type**: 'GEO_ANOMALY'
- **Risk Score**: 0.95
- **Logic**:
  ```sql
  Flags transactions in different countries
  within 2 hours (120 minutes)
  Indicates impossible physical travel
  ```
- Alert Reason includes: Country codes and time difference

**Execution Order:** FraudScoring → VelocityCheck → GeoAnomalyCheck

---

### STAGE 4: SEQ_ComplianceProcessing
Executes compliance checks (OFAC and AML)

**Compliance Rules:**

**1. SQL_OFACScreening**
- **Rule**: Screen against OFAC sanctioned entities
- **Alert Type**: 'OFAC_MATCH'
- **Category**: 'SANCTIONS'
- **Risk Level**: 'HIGH'
- **Logic**:
  ```sql
  - Fuzzy matches merchant names against OFAC sanctions list
  - Match threshold: 0.85 or higher
  - Checks merchant countries against sanctioned countries list
  - Uses fn_FuzzyMatchScore function for name matching
  ```
- Requires manual review (RequiresReview = 1)
- Stores: MatchedEntity, MatchScore

**2. SQL_AMLMonitoring**
- **Rule**: Anti-Money Laundering - Structuring Detection
- **Alert Type**: 'AML_STRUCTURING'
- **Category**: 'AML'
- **Risk Level**: 'HIGH'
- **Logic**:
  ```sql
  Detects pattern of multiple transactions just under $10K threshold
  - Looks for 3+ transactions between $9,000-$9,999
  - Within 7-day rolling window
  - Indicates potential structuring to avoid reporting requirements
  ```
- Alert includes: Transaction count, average amount, time period

**3. SQL_GetComplianceCount**
- Counts total compliance violations
- Updates ComplianceViolationCount variable

**Execution Order:** OFACScreening → AMLMonitoring → GetComplianceCount

---

### STAGE 5: SEQ_Finalization
Final cleanup and notifications

**Tasks:**

**1. SQL_MarkRecordsProcessed**
- Updates source records in `RawTransactions`
- Sets: ProcessedFlag = 1, ProcessedDateTime = NOW, ProcessedByPackageID

**2. SQL_LogPackageCompletion**
- Updates `ETL_PackageExecution` table
- Records: EndTime, Status='Completed', RecordsProcessed, RecordsFailed, FraudAlerts, ComplianceAlerts, Duration

**3. SMTP_SendCompletionNotification**
- **From**: etl-notifications@enterprise.bank
- **To**: dataengineering@enterprise.bank
- **Subject**: `[SUCCESS] Credit Card Processing - [Date] - [Count] records`
- **Content**:
  - Processing Date
  - Records Processed
  - Records Failed
  - Fraud Alerts Generated
  - Compliance Alerts Generated
  - Package Execution ID

**Execution Order:** MarkRecordsProcessed → LogPackageCompletion → SendNotification

---

## Main Control Flow Precedence Constraints

```
Initialization (Always)
    ↓
ExtractAndValidate (Always)
    ↓
FraudDetection (Conditional: EnableFraudDetection == TRUE)
    ↓
ComplianceProcessing (Conditional: EnableComplianceCheck == TRUE)
    ↓
Finalization (Always)
```

---

## Event Handlers

### OnError Event Handler
Executes when any error occurs

**Error Handling Logic:**

**1. SQL_LogError**
- Logs to: `dbo.ETL_ErrorLog`
- Captures: PackageExecutionID, PackageName, TaskName, ErrorCode, ErrorDescription, ErrorDateTime, MachineName, UserName

**2. SQL_UpdateStatusFailed**
- Updates package status to 'Failed'
- Records EndTime

**3. SMTP_SendErrorNotification**
- **From**: etl-alerts@enterprise.bank
- **To**: dataengineering@enterprise.bank;oncall@enterprise.bank
- **Subject**: `[CRITICAL] CC Processing Failed - [TaskName] - Error: [ErrorCode]`
- **Priority**: High
- **Content**: Package, Task, Error details, Machine, ExecutionID

**Execution Order:** LogError → UpdateStatusFailed → SendErrorNotification

### OnWarning Event Handler

**1. SQL_LogWarning**
- Logs to: `dbo.ETL_WarningLog`
- Captures: PackageExecutionID, PackageName, TaskName, WarningCode, WarningDescription, WarningDateTime

---

## Logging Configuration

**Log Provider:** SQL Server Log Provider
**Connection:** LOG_ErrorDatabase
**Status:** Enabled

**Events Logged:**
- OnError
- OnWarning
- OnInformation
- OnPreExecute
- OnPostExecute
- OnProgress
- OnTaskFailed

---

## Key Business Rules Summary

### Data Quality & Validation
1. **PCI-DSS Compliance**: Card numbers masked (show only first 4 and last 4 digits)
2. **Merchant Validation**: All merchant category codes validated against approved list
3. **Invalid Records**: Routed to quarantine for review

### Fraud Detection Rules
1. **High-Value Transactions**: Transactions ≥ $10,000 flagged for enhanced due diligence
2. **Velocity Check**: 5+ transactions within 5 minutes = fraud alert (Risk: 0.85)
3. **Geographic Anomaly**: Transactions in different countries within 2 hours = fraud alert (Risk: 0.95)
4. **ML Scoring**: Transactions scored against fraud threshold of 0.75

### Compliance Rules
1. **OFAC Screening**: International transactions screened against sanctions list (fuzzy match ≥ 0.85)
2. **AML Structuring**: 3+ transactions between $9,000-$9,999 within 7 days = AML alert
3. **Sanctioned Countries**: Transactions to sanctioned countries flagged

### Processing Rules
1. **Duplicate Detection**: 5-minute window for duplicate transactions
2. **Batch Processing**: 50,000 records per batch
3. **Data Retention**: 7 years per regulatory requirement

---

## SQL Queries and Stored Procedures

### Stored Procedures Called:
1. `dbo.usp_ProcessFraudDetection` - ML-based fraud scoring

### User-Defined Functions Used:
1. `dbo.fn_FuzzyMatchScore` - Fuzzy string matching for OFAC screening

### Key Tables Accessed:

**Source Tables:**
- `dbo.RawTransactions`
- `dbo.CustomerAccounts`
- `dbo.MerchantCategoryCodes`

**Target Tables:**
- `dbo.FactCreditCardTransactions` (Data Warehouse)
- `dbo.FraudAnalysisQueue`
- `dbo.FraudAlerts`
- `dbo.ComplianceAlerts`
- `dbo.TransactionQuarantine`

**Reference Tables:**
- `dbo.OFACSanctionsList`
- `dbo.SanctionedCountries`

**Logging Tables:**
- `dbo.ETL_PackageExecution`
- `dbo.ETL_ErrorLog`
- `dbo.ETL_WarningLog`

---

## Error Handling Strategy

1. **Component Level**: Error outputs redirect to quarantine destination
2. **Task Level**: OnError event handler logs and notifies
3. **Package Level**: Status tracking in execution log
4. **Failed Records**: Isolated in TransactionQuarantine table
5. **Notifications**: Critical alerts sent to on-call team

---

## Performance Optimizations

1. **Fast Load**: TABLOCK, bulk insert mode (50,000 rows per batch)
2. **Parallel Processing**: Multicast enables simultaneous loads
3. **Conditional Execution**: Feature flags prevent unnecessary processing
4. **Indexing**: Date key generation for efficient warehouse queries

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    INITIALIZATION STAGE                         │
│  Log Start → Validate Connections → Get Processing Stats       │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                 EXTRACT & VALIDATE STAGE                        │
│                                                                 │
│  Raw Transactions ──→ Add Calculated Fields ──→ Convert Types  │
│                                ↓                                │
│                    Conditional Split                            │
│              ┌─────────┴─────────┬───────────┐                │
│              ↓                   ↓           ↓                 │
│         High Value      International    Standard              │
│              └─────────┬─────────┴───────────┘                │
│                        ↓                                        │
│              Merchant Category Lookup                          │
│                   ┌────┴────┐                                  │
│                Match      No Match                             │
│                   ↓           ↓                                 │
│              Multicast    Quarantine                           │
│         ┌──────┼──────┐                                        │
│         ↓      ↓      ↓                                        │
│      DW    Fraud   Archive                                     │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                 FRAUD DETECTION STAGE                           │
│   ML Scoring → Velocity Check → Geo Anomaly Detection          │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│               COMPLIANCE PROCESSING STAGE                       │
│     OFAC Screening → AML Monitoring → Get Counts               │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                   FINALIZATION STAGE                            │
│   Mark Processed → Log Completion → Send Notification          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Critical Thresholds and Alerts

| Metric | Threshold | Action |
|--------|-----------|--------|
| Transaction Amount | ≥ $10,000 | Enhanced due diligence |
| Fraud Score | ≥ 0.75 | Fraud alert |
| Velocity | 5+ txns in 5 mins | Fraud alert (0.85) |
| Geo Anomaly | Different countries in 2 hrs | Fraud alert (0.95) |
| OFAC Match | ≥ 0.85 fuzzy match | Compliance alert |
| AML Structuring | 3+ txns $9K-$9.999 in 7 days | AML alert |

---

## Package Execution Flow Summary

1. **Initialize**: Log execution, validate connections, get pending record count
2. **Extract**: Pull raw transactions with customer data (31 fields)
3. **Transform**: Mask card numbers, add calculated fields, convert types
4. **Validate**: Check merchant categories, route invalid to quarantine
5. **Load**: Simultaneously load to DW, fraud queue, and archive
6. **Fraud Detect**: ML scoring, velocity checks, geographic anomaly detection
7. **Compliance**: OFAC screening, AML structuring detection
8. **Finalize**: Mark processed, log completion, send notifications
9. **Error Handling**: Log errors, update status, alert on-call team

---

**Document Generated:** 2025-12-07
**Source File:** CreditCardTransactionProcessing.dtsx
**Extraction Method:** Automated analysis of SSIS package XML structure
