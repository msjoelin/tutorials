import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Settings
num_users = 100000 # Increased for better cohort visibility
start_date = datetime(2023, 1, 1)

subs_data = []
for i in range(num_users):
    u_id = f"user_{1000 + i}"
    signup_date = start_date + timedelta(days=np.random.randint(0, 1095))
    
    # 1. Assign Plan Type with different price points and churn profiles
    rand = np.random.random()
    if rand < 0.70:
        plan, price, churn_p = "basic", 29.99, 0.18  # High churn
    elif rand < 0.95:
        plan, price, churn_p = "pro", 79.99, 0.10    # Medium churn
    else:
        plan, price, churn_p = "enterprise", 249.99, 0.04 # Very sticky
    
    # 2. Tenure based on the plan's specific churn probability
    tenure_months = np.random.geometric(p=churn_p)
    tenure_months = min(tenure_months, 36)
    
    for month in range(tenure_months):
        payment_date = signup_date + timedelta(days=30 * month)
        if payment_date > datetime.now():
            break
            
        subs_data.append({
            "user_id": u_id,
            "payment_id": f"pay_{u_id}_{month}",
            "payment_date": payment_date,
            "amount": price,
            "plan_type": plan
        })

df_subs = pd.DataFrame(subs_data)

# 3. Generate Refunds (Keep the 4% rate)
refund_indices = np.random.choice(df_subs.index, size=int(len(df_subs) * 0.04), replace=False)
refund_data = []

for idx in refund_indices:
    row = df_subs.loc[idx]
    is_partial = np.random.random() > 0.8
    refund_amount = round(row['amount'] * 0.3, 2) if is_partial else row['amount']
    
    refund_data.append({
        "refund_id": f"ref_{row['payment_id']}",
        "payment_id": row['payment_id'],
        "refund_date": row['payment_date'] + timedelta(days=np.random.randint(1, 15)),
        "refund_amount": refund_amount
    })

df_refunds = pd.DataFrame(refund_data)

# Export for BigQuery
df_subs.to_csv("saas_subscriptions.csv", index=False)
df_refunds.to_csv("saas_refunds.csv", index=False)
