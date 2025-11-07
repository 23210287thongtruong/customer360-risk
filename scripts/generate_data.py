import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import argparse
import os
import uuid
import hashlib

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
        # Track existing customers using email as business key
        self.existing_customers = {}  # email -> customer_record

    def generate_deterministic_customer_id(self, name, email):
        """Generate a deterministic customer ID from name and email.

        This ensures consistent IDs for the same customer across runs,
        supporting reproducible analytics and grouping."""
        # Create a hash of name + email for deterministic ID generation
        key = f"{name.lower().strip()}|{email.lower().strip()}"
        hash_obj = hashlib.md5(key.encode('utf-8'))
        # Convert first 8 bytes of hash to hex for a shorter, readable ID
        customer_id = hash_obj.hexdigest()[:16]
        return f"CUST-{customer_id.upper()}"

    def generate_customers(self):
        customers = []
        attempts = 0
        max_attempts = self.num_customers * 3

        while len(customers) < self.num_customers and attempts < max_attempts:
            attempts += 1

            name = fake.name()
            email = fake.email()

            # Skip if email already exists
            if email in self.existing_customers:
                continue

            # Use deterministic ID based on name+email for consistent grouping
            customer_id = self.generate_deterministic_customer_id(name, email)

            customer = {
                "customer_id": customer_id,
                "name": name,
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
                "address": fake.address().replace("\n", ", "),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "phone": fake.phone_number()[:20],
                "email": email,
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
            self.existing_customers[email] = customer

        self.customers_df = pd.DataFrame(customers)
        self.customers_df["annual_income"] = self.customers_df["annual_income"].clip(
            lower=15000
        )

        return self.customers_df

    def generate_transactions(self):
        if self.customers_df is None:
            raise ValueError("Must generate customers first")

        transactions = []
        transaction_types = ["purchase", "withdrawal", "deposit", "transfer", "payment"]

        for _, customer in self.customers_df.iterrows():
            customer_id = customer["customer_id"]
            num_txns = np.random.poisson(self.num_transactions_per_customer)

            # Ensure at least 1 transaction per customer
            num_txns = max(1, num_txns)

            for i in range(num_txns):
                transaction = {
                    "transaction_id": str(uuid.uuid4()),
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
                    "is_weekend": None,  # Will be calculated below
                    "is_online": np.random.choice([True, False], p=[0.3, 0.7]),
                }
                transactions.append(transaction)

        self.transactions_df = pd.DataFrame(transactions)
        self.transactions_df["is_weekend"] = (
            pd.to_datetime(self.transactions_df["timestamp"]).dt.weekday >= 5
        )
        self.transactions_df["hour"] = pd.to_datetime(
            self.transactions_df["timestamp"]
        ).dt.hour
        self.transactions_df["is_fraud"] = np.random.choice(
            [True, False], size=len(self.transactions_df), p=[0.002, 0.998]
        )

        return self.transactions_df

    def generate_credit_scores(self):
        if self.customers_df is None:
            raise ValueError("Must generate customers first")

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

        if self.transactions_df is not None:
            transactions_path = os.path.join(output_dir, "transactions.csv")
            self.transactions_df.to_csv(transactions_path, index=False)

        if self.credit_scores_df is not None:
            credit_path = os.path.join(output_dir, "credit_scores.csv")
            self.credit_scores_df.to_csv(credit_path, index=False)

    def generate_all(self, output_dir="./data/raw", simulate_incremental=False):
        self.generate_customers()
        self.generate_transactions()
        self.generate_credit_scores()

        if simulate_incremental:
            self.simulate_incremental_load()

        self.save_datasets(output_dir)

    def simulate_incremental_load(self, additional_transactions_per_customer=10):
        """
        Simulate incremental data load:
        1. Add more transactions for existing customers (append)
        2. Update credit scores for existing customers (replace)
        3. Add some new customers
        """
        if self.customers_df is None:
            raise ValueError("Must generate initial customers first")

        # 1. Add more transactions for existing customers
        additional_transactions = []
        transaction_types = ["purchase", "withdrawal", "deposit", "transfer", "payment"]

        for _, customer in self.customers_df.iterrows():
            customer_id = customer["customer_id"]
            num_new_txns = np.random.poisson(additional_transactions_per_customer)

            for i in range(num_new_txns):
                transaction = {
                    "transaction_id": str(uuid.uuid4()),
                    "customer_id": customer_id,
                    "transaction_type": np.random.choice(transaction_types),
                    "amount": round(np.random.exponential(150), 2),
                    "timestamp": fake.date_time_between(
                        start_date="-30d", end_date="now"
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
                additional_transactions.append(transaction)

        # Append new transactions
        if additional_transactions:
            new_transactions_df = pd.DataFrame(additional_transactions)
            new_transactions_df["is_weekend"] = (
                pd.to_datetime(new_transactions_df["timestamp"]).dt.weekday >= 5
            )
            new_transactions_df["hour"] = pd.to_datetime(
                new_transactions_df["timestamp"]
            ).dt.hour
            new_transactions_df["is_fraud"] = np.random.choice(
                [True, False], size=len(new_transactions_df), p=[0.002, 0.998]
            )
            self.transactions_df = pd.concat(
                [self.transactions_df, new_transactions_df], ignore_index=True
            )

        # 2. Update credit scores (business rule: replace, not append)
        for idx, row in self.credit_scores_df.iterrows():
            old_score = row["credit_score"]
            score_change = np.random.normal(0, 15)
            new_score = max(300, min(850, int(old_score + score_change)))

            self.credit_scores_df.loc[idx, "credit_score"] = new_score
            self.credit_scores_df.loc[idx, "score_date"] = fake.date_between(
                start_date="-30d", end_date="today"
            )

        # 3. Add some new customers
        new_customer_count = max(
            1, int(len(self.customers_df) * 0.05)
        )  # 5% new customers
        for _ in range(new_customer_count):
            email = fake.email()
            if email not in self.existing_customers:
                name = fake.name()
                customer_id = self.generate_deterministic_customer_id(name, email)
                customer = {
                    "customer_id": customer_id,
                    "name": name,
                    "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
                    "address": fake.address().replace("\n", ", "),
                    "city": fake.city(),
                    "state": fake.state(),
                    "zip_code": fake.zipcode(),
                    "phone": fake.phone_number()[:20],
                    "email": email,
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
                    "created_date": fake.date_between(
                        start_date="-30d", end_date="today"
                    ),
                }

                # Add new customer
                self.existing_customers[email] = customer
                new_customer_df = pd.DataFrame([customer])
                new_customer_df["annual_income"] = new_customer_df[
                    "annual_income"
                ].clip(lower=15000)
                self.customers_df = pd.concat(
                    [self.customers_df, new_customer_df], ignore_index=True
                )

                # Add credit score for new customer
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
                credit_record = pd.DataFrame(
                    [
                        {
                            "customer_id": customer_id,
                            "credit_score": credit_score,
                            "score_date": fake.date_between(
                                start_date="-30d", end_date="today"
                            ),
                            "credit_history_length": np.random.randint(1, 20),
                            "number_of_accounts": np.random.randint(1, 15),
                            "total_debt": round(np.random.exponential(25000), 2),
                            "credit_utilization": round(
                                np.random.uniform(0.05, 0.85), 2
                            ),
                        }
                    ]
                )

                self.credit_scores_df = pd.concat(
                    [self.credit_scores_df, credit_record], ignore_index=True
                )


def main():
    parser = argparse.ArgumentParser(
        description="Generate Customer 360 synthetic datasets"
    )
    parser.add_argument(
        "--customers", type=int, default=10000, help="Number of customers to generate"
    )
    parser.add_argument(
        "--transactions", type=int, default=50, help="Average transactions per customer"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./data/raw",
        help="Output directory for CSV files",
    )
    parser.add_argument(
        "--incremental", action="store_true", help="Simulate incremental data load"
    )

    args = parser.parse_args()

    generator = Customer360DataGenerator(
        num_customers=args.customers, num_transactions_per_customer=args.transactions
    )
    generator.generate_all(args.output, simulate_incremental=args.incremental)
    print("Data generation completed!")


if __name__ == "__main__":
    main()
