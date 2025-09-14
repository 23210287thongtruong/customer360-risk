import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import argparse
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


class Customer360DataGenerator:
    def __init__(self, num_customers=10000, num_transactions_per_customer=50):
        self.num_customers = num_customers
        self.num_transactions_per_customer = num_transactions_per_customer
        self.customers_df = None
        self.transactions_df = None
        self.credit_scores_df = None

    def generate_customers(self):
        print(f"Generating {self.num_customers} customer records...")

        customers = []
        for i in range(1, self.num_customers + 1):
            customer_id = f"CUST_{i:06d}"[:20]
            phone = fake.phone_number()[:20]

            customer = {
                "customer_id": customer_id,
                "name": fake.name(),
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
                "address": fake.address().replace("\n", ", "),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "phone": phone,
                "email": fake.email(),
                "annual_income": round(np.random.normal(65000, 25000), 2),
                "job_title": fake.job(),
                "employment_status": np.random.choice(
                    ["Full-time", "Part-time", "Self-employed", "Unemployed"],
                    p=[0.7, 0.15, 0.1, 0.05],
                ),
                "marital_status": np.random.choice(
                    ["Single", "Married", "Divorced", "Widowed"],
                    p=[0.4, 0.45, 0.1, 0.05],
                ),
                "created_date": fake.date_between(start_date="-5y", end_date="today"),
            }
            customers.append(customer)

        self.customers_df = pd.DataFrame(customers)
        self.customers_df["annual_income"] = self.customers_df["annual_income"].clip(
            lower=15000
        )
        return self.customers_df

    def generate_transactions(self):
        if self.customers_df is None:
            raise ValueError("Must generate customers first")

        print(f"Generating transactions for {self.num_customers} customers...")

        transactions = []
        transaction_types = ["purchase", "withdrawal", "deposit", "transfer", "payment"]

        for _, customer in self.customers_df.iterrows():
            customer_id = customer["customer_id"]
            num_txns = np.random.poisson(self.num_transactions_per_customer)

            for i in range(num_txns):
                transaction = {
                    "transaction_id": f"TXN_{customer_id}_{i + 1:04d}",
                    "customer_id": customer_id,
                    "transaction_type": np.random.choice(transaction_types),
                    "amount": round(np.random.exponential(150), 2),
                    "timestamp": fake.date_time_between(
                        start_date="-2y", end_date="now"
                    ),
                    "merchant_name": fake.company(),
                    "merchant_category": np.random.choice(
                        [
                            "Grocery",
                            "Gas",
                            "Restaurant",
                            "Retail",
                            "Entertainment",
                            "Healthcare",
                            "Utilities",
                            "Online",
                            "Travel",
                        ]
                    ),
                    "location_city": fake.city(),
                    "location_state": fake.state(),
                    "is_weekend": None,
                    "is_online": np.random.choice([True, False], p=[0.3, 0.7]),
                }
                transactions.append(transaction)

        self.transactions_df = pd.DataFrame(transactions)
        self.transactions_df["is_weekend"] = (
            self.transactions_df["timestamp"].dt.weekday >= 5
        )
        self.transactions_df["hour"] = self.transactions_df["timestamp"].dt.hour
        self.transactions_df["is_fraud"] = np.random.choice(
            [True, False], size=len(self.transactions_df), p=[0.002, 0.998]
        )
        return self.transactions_df

    def generate_credit_scores(self):
        if self.customers_df is None:
            raise ValueError("Must generate customers first")

        print(f"Generating credit scores for {self.num_customers} customers...")

        credit_scores = []
        for _, customer in self.customers_df.iterrows():
            income = customer["annual_income"]

            if income > 80000:
                base_score = np.random.normal(750, 50)
            elif income > 50000:
                base_score = np.random.normal(680, 60)
            elif income > 30000:
                base_score = np.random.normal(620, 70)
            else:
                base_score = np.random.normal(550, 80)

            credit_score = max(300, min(850, int(base_score)))

            credit_record = {
                "customer_id": customer["customer_id"],
                "credit_score": credit_score,
                "score_date": fake.date_between(start_date="-1y", end_date="today"),
                "credit_history_length": np.random.randint(1, 20),
                "number_of_accounts": np.random.randint(1, 15),
                "total_debt": round(np.random.exponential(25000), 2),
                "credit_utilization": round(np.random.uniform(0.05, 0.85), 2),
            }
            credit_scores.append(credit_record)

        self.credit_scores_df = pd.DataFrame(credit_scores)
        return self.credit_scores_df

    def save_datasets(self, output_dir="./data/raw"):
        os.makedirs(output_dir, exist_ok=True)

        if self.customers_df is not None:
            customers_path = os.path.join(output_dir, "customers.csv")
            self.customers_df.to_csv(customers_path, index=False)
            print(f"Saved customers dataset to {customers_path}")

        if self.transactions_df is not None:
            transactions_path = os.path.join(output_dir, "transactions.csv")
            self.transactions_df.to_csv(transactions_path, index=False)
            print(f"Saved transactions dataset to {transactions_path}")

        if self.credit_scores_df is not None:
            credit_path = os.path.join(output_dir, "credit_scores.csv")
            self.credit_scores_df.to_csv(credit_path, index=False)
            print(f"Saved credit scores dataset to {credit_path}")

    def generate_all(self, output_dir="./data/raw"):
        self.generate_customers()
        self.generate_transactions()
        self.generate_credit_scores()
        self.save_datasets(output_dir)

        print(
            f"\nGenerated {len(self.customers_df):,} customers, "
            f"{len(self.transactions_df):,} transactions, "
            f"{len(self.credit_scores_df):,} credit scores"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Generate Customer 360 synthetic datasets"
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=10000,
        help="Number of customers to generate (default: 10000)",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=50,
        help="Average transactions per customer (default: 50)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./data/raw",
        help="Output directory for CSV files (default: ./data/raw)",
    )

    args = parser.parse_args()

    generator = Customer360DataGenerator(
        num_customers=args.customers, num_transactions_per_customer=args.transactions
    )
    generator.generate_all(args.output)
    print("Data generation completed!")


if __name__ == "__main__":
    main()
