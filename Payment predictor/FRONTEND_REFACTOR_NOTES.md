# Frontend Refactor - Input Section Simplification

## Changes Made

### 1. **Updated `templates/index.html`**
   - **Removed:** "Monthly Operating Cost" input field
   - **Added:** "Periode Waktu" (Time Period) dropdown selector
   - **Kept:** "Cash on Hand Saat Ini" (Current Cash on Hand) number input
   - **Added:** Optional "Catatan Tambahan" (Additional Notes) textarea

### 2. **User Inputs (Frontend Only)**
   ```
   ✓ Periode Waktu      - Dropdown (week-based: 1-10, 11-20, 21-akhir bulan)
   ✓ Cash On Hand       - Number input (currently IDR 500M default)
   ✓ Catatan Tambahan   - Optional textarea for analysis focus
   ```

### 3. **Backend Data (Not User Input)**
   Backend provides these from database/configuration:
   ```
   - Invoices & payment history
   - Sales pipeline data
   - Monthly operating cost (fixed at IDR 200M in config)
   - Customer behavior profiles (Kelas A-E)
   - Payment behavior analytics
   ```

## API Endpoint Updates

### `/api/forecast/by-horizon` (POST)
**Request Body (Frontend sends):**
```json
{
  "start_date": "2026-03-31",
  "cash_on_hand": 500000000,
  "additional_notes": "Fokus pada klien risiko tinggi"
}
```

**Response:**
- `short_term` (0-30 days): Liquidity & cash deficit focus
- `mid_term` (1-3 months): Operational stability focus  
- `long_term` (3-12 months): Growth planning focus

## Frontend Architecture

### Data Flow
```
User Input (Periode + Cash + Notes)
         ↓
   Frontend Form
         ↓
   API Request
         ↓
   Flask Backend (app.py)
         ↓
   CashflowForecaster Engine
         ↓
   All 3 Time Horizons Processed
         ↓
   Multi-Horizon Response
         ↓
   Tab-Based UI Display
```

### UI Structure
1. **Input Section** - Accepts user inputs only (minimal)
2. **Time Horizon Tabs** - Short/Mid/Long term navigation
3. **Dashboard** - Shows current status, outstanding analysis, alerts, recommendations
4. **Chart** - Projected cash inflows by predicted payment date
5. **Recommendations** - Prioritized by SoP, retention, satisfaction

## Implementation Notes

### Monthly Operating Cost Handling
- **Frontend:** NOT requested from user
- **Backend:** Defaults to IDR 200M (configurable in app.py line 607)
- **Benefit:** Ensures consistency, prevents user misconfiguration

### Period Selection Logic
- Periods are loaded via `/api/forecast/periods` endpoint
- Returns week-based segments: "1-10 Jan", "11-20 Jan", "21-31 Jan", etc.
- User selects from dropdown, frontend extracts start/end dates

### Additional Notes Field
- Optional text input for user to provide analysis context
- Passed to backend for potential future analysis focus
- Non-blocking field (API doesn't require it)

## Verification Checklist
✅ forecast_engine.py imports successfully  
✅ app.py Flask initialization successful  
✅ `/api/forecast/by-horizon` endpoint handles missing monthly_operating_cost (uses default)  
✅ HTML form structure matches user input requirements  
✅ Tab-based UI ready for 3-horizon navigation  

## Next Steps (If Needed)
1. Test the full flow by running the app and making a forecast request
2. Verify period dropdown populates correctly from `/api/forecast/periods`
3. Test tab switching between short/mid/long term views
4. Validate alert/recommendation rendering for different horizons
