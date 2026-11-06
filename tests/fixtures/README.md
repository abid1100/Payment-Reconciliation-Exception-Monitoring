# Test Fixtures & Independent Ground Truth Matrix

**Purpose:** Independent specification of expected test outcomes.  
**Critical Testing Rule:** Tests must assert against these hardcoded ground-truth values and never calculate expected answers by calling production reconciliation or transformation functions.

---

## 1. Fixture Inventory

The test fixtures cover 12 isolated test conditions across three time anchors:
* **Time Anchor $T_0$:** `2026-01-01T00:00:00Z`
* **Evaluation Time $A_1$ (In-Window):** `2026-01-03T12:00:00Z` ($\Delta t = 60\text{h} < 72\text{h}$)
* **Evaluation Time $A_2$ (Overdue):** `2026-01-07T00:00:00Z` ($\Delta t = 144\text{h} > 72\text{h}$)

---

## 2. Independent Assertion Matrix

| Case ID | Subject Key | Rule Tested | Evaluation Time | Expected State | Expected Reason Code | Expected Difference (`paise`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-01** | `ord_101` | `ORDER_PAYMENT` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-02** | `txn_301` | `PAYMENT_ORDER` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-03** | `txn_301` | `TRANSACTION_SETTLEMENT` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-04** | `stl_501` | `SETTLEMENT_ARITHMETIC` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-05** | `rf_303` | `REFUND_PARENT` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-06** | `txn_302` | `REFUND_LIMIT` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-07** | `po_601` | `PAYOUT_COMPLETENESS` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-08** | `po_601` | `PAYOUT_TOTAL` | $A_1$ or $A_2$ | `MATCHED` | `MATCH_OK` | `0` |
| **TC-09** | `ord_103` | `TRANSACTION_SETTLEMENT` | $A_1$ ($+3\text{h}$) | `PENDING` | `AWAITING_SETTLEMENT` | `NULL` |
| **TC-10** | `ord_103` | `TRANSACTION_SETTLEMENT` | $A_2$ ($+87\text{h}$) | `EXCEPTION` | `SETTLEMENT_OVERDUE` | `NULL` |
| **TC-11** | `ord_104` | `ORDER_PAYMENT` | $A_1$ or $A_2$ | `EXCEPTION` | `ORDER_AMOUNT_MISMATCH` | `-50000` |
| **TC-12** | `evt_201` | Deduplication | Ingestion | Row 1: `ACCEPTED`<br/>Row 2: `DUPLICATE` | `EXACT_PAYLOAD_MATCH` | `0` |

---

## 3. Calculation Ground Truth Verification

### 3.1 Payout Mathematical Reconciliation
$$\text{Expected Payout Net} = 636000 \text{ paise}$$
$$\text{Observed Net Sum} = (+490000) + (+196000) + (-50000) = 636000 \text{ paise}$$
$$\text{Discrepancy} = 636000 - 636000 = 0 \text{ paise (MATCHED)}$$

### 3.2 Order Undercharge Discrepancy
$$\text{Expected Order Amount} = 300000 \text{ paise}$$
$$\text{Observed Captured Amount} = 250000 \text{ paise}$$
$$\text{Difference} = \text{Observed} - \text{Expected} = 250000 - 300000 = -50000 \text{ paise (EXCEPTION)}$$

### 3.3 Refund Limit Calculation
$$\text{Captured Amount (`txn_302`)} = 200000 \text{ paise}$$
$$\text{Refund Amount (`rf_303`)} = 50000 \text{ paise}$$
$$\text{Remaining Capture Available for Refund} = 200000 - 50000 = 150000 \text{ paise} \ge 0 \text{ (MATCHED)}$$
