import pandas as pd
import numpy as np
import os

print("Генерация данных. Это займет около 1-2 минут...")

n_rows = 1500000 # 1.5 млн строк дадут примерно 130 МБ

np.random.seed(42)

# Генерация фичей
customer_id = np.arange(1, n_rows + 1)
tenure_months = np.random.randint(1, 72, size=n_rows)
monthly_charges = np.random.uniform(20.0, 120.0, size=n_rows)
total_charges = tenure_months * monthly_charges * np.random.uniform(0.9, 1.1, size=n_rows)
tech_support_calls = np.random.poisson(lam=1.5, size=n_rows)
contract_type = np.random.choice(['Month-to-month', 'One year', 'Two year'], size=n_rows, p=[0.5, 0.3, 0.2])

# Логика оттока (Churn): чаще уходят клиенты с коротким стажем, высокими платежами и частыми звонками в саппорт
churn_prob = np.zeros(n_rows)
churn_prob += np.where(tenure_months < 12, 0.3, 0)
churn_prob += np.where(monthly_charges > 80, 0.2, 0)
churn_prob += np.where(tech_support_calls > 3, 0.4, 0)
churn_prob += np.where(contract_type == 'Month-to-month', 0.2, -0.2)

# Нормализация вероятностей и добавление шума
churn_prob = np.clip(churn_prob + np.random.normal(0, 0.1, size=n_rows), 0, 1)
churn = np.where(churn_prob > 0.5, 'Yes', 'No')

# Сборка DataFrame
df = pd.DataFrame({
    'CustomerID': customer_id,
    'TenureMonths': tenure_months,
    'MonthlyCharges': monthly_charges,
    'TotalCharges': total_charges,
    'TechSupportCalls': tech_support_calls,
    'ContractType': contract_type,
    'Churn': churn
})

output_path = 'data/telecom_churn.csv'
df.to_csv(output_path, index=False)

file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
print(f"Данные успешно сгенерированы! Файл сохранен в {output_path}")
print(f"Количество строк: {n_rows}")
print(f"Размер файла: {file_size_mb:.2f} MB")