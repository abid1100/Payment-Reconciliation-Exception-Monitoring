# Tiny Synthetic Demonstration Sample Dataset

**Schema Version:** 1.0.0  
**Batch ID:** `batch_sample_001`  
**Scenario:** `sample_baseline_with_edge_cases`  
**Seed:** 42  
**Base Time:** `2026-01-01T00:00:00Z`  
**Delivered At:** `2026-01-03T10:00:00Z`  
**Currency:** `INR` (all amounts in minor units / paise)  

---

## 1. Provenance and Synthetic Data Notice

All records in this directory are purely synthetic, generated for portfolio demonstration and deterministic automated verification.
* **No Real Customers or Financial Data:** No bank accounts, customer identifiers, real card details, or live payment gateway accounts are contained in these files.
* **Integrity Guarantee:** All file content hashes in `manifest.json` are cryptographically verified SHA-256 digests of the actual file bytes.

---

## 2. Included Transactions & Hand-Checkable Walkthrough

This small dataset contains 4 checkout orders, 6 delivered webhook events (including 1 duplicate delivery), 3 settlement clearing lines, and 1 summary payout record.

### Scenario 1: Clean, Perfect 3-Way Match (`ord_101`)
* **Order:** `ord_101` for ₹5,000.00 (`500000` paise).
* **Payment Event:** `evt_201` capturing ₹5,000.00 (`500000` paise) under transaction `txn_301`.
* **Settlement Line:** `stl_501` reporting gross ₹5,000.00 (`500000`), processor fee ₹100.00 (`10000`), and net ₹4,900.00 (`490000`).
* **Expected Outcome:** `MATCHED` on `ORDER_PAYMENT`, `PAYMENT_ORDER`, `TRANSACTION_SETTLEMENT`, and `SETTLEMENT_ARITHMETIC`.

### Scenario 2: Webhook Duplicate Delivery (`evt_201`)
* **First Delivery:** `evt_201` received at `2026-01-01T10:01:05Z`.
* **Second Delivery:** `evt_201` received at `2026-01-01T10:05:00Z` with identical business payload.
* **Expected Outcome:** First delivery loaded as canonical. Second delivery flagged as `DUPLICATE` in `raw.raw_records`. Transaction count remains exactly 1 with no double-counting of financial balances.

### Scenario 3: Order with Authorized Partial Refund (`ord_102`)
* **Order:** `ord_102` for ₹2,000.00 (`200000` paise).
* **Payment Capture:** `evt_202` capturing ₹2,000.00 (`200000` paise) under transaction `txn_302`.
* **Settlement Line (Payment):** `stl_502` gross ₹2,000.00 (`200000`), fee ₹40.00 (`4000`), net ₹1,960.00 (`196000`).
* **Refund Event:** `evt_203` issued against `txn_302` for ₹500.00 (`50000` paise) under refund transaction `rf_303`.
* **Settlement Line (Refund):** `stl_503` gross -₹500.00 (`-50000`), fee `0`, net -₹500.00 (`-50000`).
* **Expected Outcome:** 
  * `REFUND_PARENT`: `MATCHED` (`rf_303` linked to valid parent `txn_302`).
  * `REFUND_LIMIT`: `MATCHED` (cumulative refunds ₹500.00 $\le$ ₹2,000.00 captured).
  * `TRANSACTION_SETTLEMENT`: `MATCHED` for both parent payment and refund lines.

### Scenario 4: Payout Header Mathematical Balance (`po_601`)
* **Summary Payout:** `po_601` expected 3 settlement lines, reported net payout ₹6,360.00 (`636000` paise).
* **Net Settlement Lines in Batch:**
  * `stl_501`: +₹4,900.00 (`490000`)
  * `stl_502`: +₹1,960.00 (`196000`)
  * `stl_503`: -₹500.00 (`-50000`)
  * **Sum:** $490000 + 196000 - 50000 = 636000$ paise (₹6,360.00).
* **Expected Outcome:** `PAYOUT_COMPLETENESS` = `MATCHED` (3 of 3 lines observed), `PAYOUT_TOTAL` = `MATCHED` (difference = 0).

### Scenario 5: In-Flight Transaction / Pending Settlement (`ord_103`)
* **Order:** `ord_103` for ₹1,500.00 (`150000` paise) placed on `2026-01-03T09:00:00Z`.
* **Payment Capture:** `evt_204` capturing ₹1,500.00 under transaction `txn_304`.
* **Settlement Line:** Not yet received (in-flight $T+2$ banking delay).
* **Expected Outcome:**
  * When evaluated at `as_of = 2026-01-03T12:00:00Z` (age 3 hours < 72h): `PENDING` (`AWAITING_SETTLEMENT`).
  * When evaluated at `as_of = 2026-01-07T00:00:00Z` (age 87 hours > 72h): `EXCEPTION` (`SETTLEMENT_OVERDUE`).

### Scenario 6: Customer Undercharge / Amount Mismatch (`ord_104`)
* **Order:** `ord_104` expecting ₹3,000.00 (`300000` paise).
* **Payment Capture:** `evt_205` captured only ₹2,500.00 (`250000` paise) under transaction `txn_305`.
* **Expected Outcome:** `ORDER_PAYMENT` evaluates immediately to `EXCEPTION` (`ORDER_AMOUNT_MISMATCH`) with $\text{difference\_minor} = 250000 - 300000 = -50000$ paise (-₹500.00 shortfall).

---

## 3. How to Regenerate

This sample dataset is reproducible. When generator CLI is implemented:
```bash
python -m reconciliation generate \
  --scenario sample_baseline_with_edge_cases \
  --seed 42 \
  --base-time 2026-01-01T00:00:00Z \
  --output data/sample
```
