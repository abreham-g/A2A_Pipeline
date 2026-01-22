# Historical Scan Documentation

## Purpose
Run a one-off "historical" RocketSource scan for a bounded set of ASINs from the database and ingest results into the local united_state table with comprehensive historical data analysis.

## Use Cases
- **Ad-hoc historical checks** of up to N ASINs (N = ROCKETSOURCE_ASIN_LIMIT, default 1000)
- **Quick manual runs** where you want raw and normalized CSVs locally for inspection
- **Historical data analysis** including 30/90 day price trends, BSR tracking, and sales metrics
- **Data validation** or batch analysis on specific ASINs
- **Marketplace intelligence** with historical performance indicators

## Primary Script
- `historical_scan.py` - Main entry point for historical scans

## How It Works

### Workflow Summary
1. **Database Selection**: Select ASINs from Tirhak source, left-join with Umair for seller info
2. **CSV Preparation**: Build input CSV in-memory from selected ASINs
3. **Upload**: Upload CSV to RocketSource via API with historical data enabled
4. **Scan Creation**: Create and start scan, poll for completion
5. **Results Download**: Download raw results as CSV
6. **Normalization**: Process results into standardized format with historical columns
7. **Database Upsert**: Insert/update results into united_state table
8. **File Export**: Save raw and normalized CSVs locally

### Detailed Flow
```
DB Query → CSV Generation → API Upload → Scan Polling → 
Results Download → Normalization → DB Upsert → File Export
```

## Historical Data Features

### Enabled Historical Columns
When `ROCKETSOURCE_SCAN_PAYLOAD` includes `"pull_historical_data": true`, the scan captures:
- **Amazon Monthly Sold** - Monthly sales volume estimates
- **Inbound Placement Fee** - Amazon fulfillment placement costs
- **Return Rate** - Product return percentage
- **Errors** - Any processing errors or warnings
- **Purchase Order Amazon Quantity** - PO quantities for Amazon
- **Purchase Order Supplier Quantity** - PO quantities from supplier
- **Purchase Order Subtotal** - Total PO value
- **Average Price 30d** - 30-day average selling price
- **Average Price 90d** - 90-day average selling price
- **Average BSR 30d** - 30-day average Best Seller Rank
- **Average BSR 90d** - 90-day average Best Seller Rank

### Data Processing
- **Safe parsing**: All numeric fields are parsed as Decimal with fallback to 0.00
- **Error handling**: Missing or invalid data defaults to sensible values
- **Column mapping**: Multiple fallback column names for robust data extraction

## How to Run

### Setup Environment
```bash
# Activate virtual environment (Windows)
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration
Key environment variables in `.env`:

```bash
# Historical scan configuration
ROCKETSOURCE_ASIN_LIMIT=1000                    # Max ASINs per scan
ROCKETSOURCE_SCAN_PAYLOAD='{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"US","name":"Historical Scan","pull_historical_data":true}}'

# Database configuration
ROCKETSOURCE_TARGET_SCHEMA="keepa_scrape"
ROCKETSOURCE_UNITED_STATE_TABLE="test_historical_scan united_state"
ROCKETSOURCE_UNGATED_TABLE="test_tools_ungated"

# Source tables
ROCKETSOURCE_TIRHAK_SCHEMA="keepa_scrape"
ROCKETSOURCE_TIRHAK_TABLE="test_tirhak_gating"
ROCKETSOURCE_UMAIR_SCHEMA="keepa_scrape"
ROCKETSOURCE_UMAIR_TABLE="test_umair_gating"
```

### Run the Scan

#### Basic Run
```bash
python historical_scan.py
```

#### Verbose Run
```bash
python historical_scan.py --verbose
```

#### Custom Limit
```bash
python historical_scan.py --limit 500
```

## Output Files

### Raw Results
- **Location**: `Data/historical_results_{scan_id}_{timestamp}.csv`
- **Format**: Raw RocketSource API response with all available columns
- **Purpose**: Debugging and data inspection

### Normalized Results
- **Location**: `Data/historical_results_normalized_{scan_id}_{timestamp}.csv`
- **Format**: Standardized schema with historical columns
- **Purpose**: Consistent data analysis and database ingestion

### Normalized Schema
```csv
ASIN,US_BB_Price,Package_Weight,FBA_Fee,Referral_Fee,Shipping_Cost,Category,
Amazon Monthly Sold,Inbound Placement Fee,Return Rate,Errors,
Purchase Order Amazon Quantity,Purchase Order Supplier Quantity,Purchase Order Subtotal,
Average Price 30d,Average Price 90d,Average BSR 30d,Average BSR 90d,
created_at,last_updated,Seller
```

## Database Integration

### Target Table
- **Schema**: `ROCKETSOURCE_TARGET_SCHEMA` (default: "keepa_scrape")
- **Table**: `ROCKETSOURCE_UNITED_STATE_TABLE` (default: "test_historical_scan united_state")

### Table Schema
```sql
CREATE TABLE "test_historical_scan united_state" (
    "ASIN" character varying NOT NULL,
    "US_BB_Price" numeric NOT NULL DEFAULT 0,
    "Package_Weight" numeric NOT NULL DEFAULT 0,
    "FBA_Fee" numeric NOT NULL DEFAULT 0,
    "Referral_Fee" numeric NOT NULL DEFAULT 0,
    "Shipping_Cost" numeric NOT NULL DEFAULT 0,
    "Category" character varying,
    "Amazon Monthly Sold" numeric DEFAULT 0,
    "Inbound Placement Fee" numeric DEFAULT 0,
    "Return Rate" numeric DEFAULT 0,
    "Errors" character varying,
    "Purchase Order Amazon Quantity" numeric DEFAULT 0,
    "Purchase Order Supplier Quantity" numeric DEFAULT 0,
    "Purchase Order Subtotal" numeric DEFAULT 0,
    "Average Price 30d" numeric DEFAULT 0,
    "Average Price 90d" numeric DEFAULT 0,
    "Average BSR 30d" numeric DEFAULT 0,
    "Average BSR 90d" numeric DEFAULT 0,
    "created_at" timestamp without time zone,
    "last_updated" timestamp without time zone,
    "Seller" character varying,
    CONSTRAINT "united_state_pkey" PRIMARY KEY ("ASIN")
);
```

### Upsert Behavior
- **Primary Key**: ASIN
- **Conflict Resolution**: Updates all columns except Seller (preserves existing Seller value)
- **Batch Processing**: 1000 rows per batch for performance
- **Error Handling**: Falls back to row-by-row insertion on batch failures

## Troubleshooting

### Common Issues

#### Historical Columns Coming as Null/Empty
**Cause**: RocketSource API doesn't return historical data when `"pull_historical_data": false`
**Solution**: Ensure `ROCKETSOURCE_SCAN_PAYLOAD` includes `"pull_historical_data": true`

```bash
# Check current payload
echo $ROCKETSOURCE_SCAN_PAYLOAD

# Should output:
# {"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"US","name":"Historical Scan","pull_historical_data":true}}
```

#### Database Connection Errors
**Cause**: Missing or incorrect DATABASE_URL
**Solution**: Verify database connection string in `.env`

#### Missing Columns in CSV
**Cause**: RocketSource API response format changed
**Solution**: Check raw results CSV for actual column names and update mapping

### Debug Mode
Run with verbose logging to see detailed processing information:
```bash
python historical_scan.py --verbose
```

### Check API Response
Examine the raw results CSV to verify what data RocketSource is actually returning:
```bash
# Find latest raw results
ls -la Data/historical_results_*.csv | tail -1

# Check column headers
head -1 Data/historical_results_{latest}.csv
```

## Performance Considerations

### API Limits
- **Rate Limiting**: Respects RocketSource API rate limits
- **Timeout**: 10-minute default timeout for scan completion
- **Batch Size**: 1000 ASINs per scan (configurable)

### Database Performance
- **Batch Inserts**: 1000 rows per transaction
- **Connection Pooling**: Reuses database connections
- **Statement Timeout**: 60-second query timeout

### File Management
- **Temporary Files**: Uses system temp directory for processing
- **Cleanup**: Automatic cleanup of temporary files
- **Export Files**: Raw and normalized CSVs saved in Data/ directory

## Advanced Usage

### Custom Column Mapping
Modify `rocketsource_automation.py` to add new column mappings:

```python
# Add to _normalize_results_csv method
"New Column": pick(row, ["New Column", "Alternative Name", "Fallback"]),
```

### Custom SQL Queries
Update `db_service.py` to modify ASIN selection logic:

```python
# Modify _upsert_ungated_rows_sql method for different selection criteria
```

### Different Marketplaces
Update the scan payload for different marketplaces:

```bash
# Canada
ROCKETSOURCE_SCAN_PAYLOAD='{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"CA","name":"Canada Historical Scan","pull_historical_data":true}}'

# Germany
ROCKETSOURCE_SCAN_PAYLOAD='{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"DE","name":"Germany Historical Scan","pull_historical_data":true}}'
```
