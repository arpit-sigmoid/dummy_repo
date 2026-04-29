import pandas as pd

# Load CSV files
employees = pd.read_csv("/Users/as-mac-1241/PycharmProjects/promptEngineering/data/employees.csv")
departments = pd.read_csv("/Users/as-mac-1241/PycharmProjects/promptEngineering/data/department.csv")

# Join (merge) on department_id
merged = pd.merge(employees, departments, on="department_id", how="inner")

# Save to new CSV
merged.to_csv("employees_with_departments.csv", index=False)

print("Joined file saved as employees_with_departments.csv")
